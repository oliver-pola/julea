/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2018 Michael Kuhn
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <bson.h>

#include <object/jtransformation-object.h>
#include <object/jobject.h>

#include <julea.h>
#include <julea-internal.h>

/**
 * \defgroup JTransformationObject Object
 *
 * Data structures and functions for managing transformation objects.
 *
 * @{
 **/

struct JTransformationObjectOperation
{
	union
	{
		struct
		{
			JTransformationObject* object;
			gint64* modification_time;
			guint64* size;
		}
		status;

		struct
		{
			JTransformationObject* object;
			gpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_read;
		}
		read;

		struct
		{
			JTransformationObject* object;
			gconstpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_written;
		}
		write;
	};
};

typedef struct JTransformationObjectOperation JTransformationObjectOperation;

/**
 * A JTransformationObject.
 **/
struct JTransformationObject
{
    /**
     * The underlying Object
     **/
    JObject* object;
	/**
	 * The reference count.
	 **/
	gint ref_count;

    /**
     * The Transformation
     **/
    JTransformation* transformation;
};

/**
 * A JObject.
 **/
struct JObject
{
	/**
	 * The data server index.
	 */
	guint32 index;

	/**
	 * The namespace.
	 **/
	gchar* namespace;

	/**
	 * The name.
	 **/
	gchar* name;

	/**
	 * The reference count.
	 **/
	gint ref_count;

    /**
     * The Transformation
     **/
    JTransformation* transformation;
};

static
void
j_transformation_object_read_free (gpointer data)
{
	JTransformationObjectOperation* operation = data;

	j_transformation_object_unref(operation->read.object);

	if(operation->read.object->transformation != NULL)
    {
		j_transformation_cleanup(operation->read.object->transformation,
			operation->read.data, operation->read.length, operation->read.offset,
			J_TRANSFORMATION_CALLER_CLIENT_READ);
    }
	g_slice_free(JTransformationObjectOperation, operation);
}

static
void
j_transformation_object_write_free (gpointer data)
{
	JTransformationObjectOperation* operation = data;

	j_transformation_object_unref(operation->write.object);

    if(operation->write.object->transformation != NULL)
    {
		j_transformation_cleanup(operation->write.object->transformation,
			operation->write.data, operation->write.length, operation->write.offset,
			J_TRANSFORMATION_CALLER_CLIENT_WRITE);
    }
	g_slice_free(JTransformationObjectOperation, operation);
}

static
gboolean
j_transformation_object_read_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = TRUE;

	JBackend* object_backend;
	JListIterator* it;
	g_autoptr(JMessage) message = NULL;
	JObject* object;
    JTransformationObject* transformation_object;
	gpointer object_handle;
    JTransformation* transformation = NULL;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JTransformationObjectOperation* operation = j_list_get_first(operations);

        transformation_object = operation->status.object;
		object = transformation_object->object;

        transformation = transformation_object->transformation;

		g_assert(operation != NULL);
		g_assert(object != NULL);
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_backend();

	if (object_backend != NULL)
	{
		ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
	}
	else
	{
		gsize name_len;
		gsize namespace_len;

		namespace_len = strlen(object->namespace) + 1;
		name_len = strlen(object->name) + 1;

		message = j_message_new(J_MESSAGE_OBJECT_READ, namespace_len + name_len);
		j_message_set_safety(message, semantics);
		j_message_append_n(message, object->namespace, namespace_len);
		j_message_append_n(message, object->name, name_len);
	}

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("item", path);
	}
	*/

	while (j_list_iterator_next(it))
	{
		JTransformationObjectOperation* operation = j_list_iterator_get(it);
		gpointer data = operation->read.data;
		guint64 length = operation->read.length;
		guint64 offset = operation->read.offset;
		guint64* bytes_read = operation->read.bytes_read;

        transformation = transformation_object->transformation;

		j_trace_file_begin(object->name, J_TRACE_FILE_READ);

		if (object_backend != NULL)
		{
			guint64 nbytes = 0;

			ret = j_backend_object_read(object_backend, object_handle, data, length, offset, &nbytes) && ret;
			j_helper_atomic_add(bytes_read, nbytes);

            // Transform the read data if the object has a transformation set
            if(transformation != NULL)
            {
                j_transformation_apply(transformation, &data, &length, &offset,
					J_TRANSFORMATION_CALLER_CLIENT_READ);
				// data, length, offset must not change here
            }
		}
		else
		{
			j_message_add_operation(message, sizeof(guint64) + sizeof(guint64));
			j_message_append_8(message, &length);
			j_message_append_8(message, &offset);
		}

		j_trace_file_end(object->name, J_TRACE_FILE_READ, length, offset);
	}

	j_list_iterator_free(it);

	if (object_backend != NULL)
	{
		ret = j_backend_object_close(object_backend, object_handle) && ret;
	}
	else
	{
		g_autoptr(JMessage) reply = NULL;
		GSocketConnection* object_connection;
		guint32 operations_done;
		guint32 operation_count;

		object_connection = j_connection_pool_pop_object(object->index);
		j_message_send(message, object_connection);

		reply = j_message_new_reply(message);

		operations_done = 0;
		operation_count = j_message_get_count(message);

		it = j_list_iterator_new(operations);

		/**
		 * This extra loop is necessary because the server might send multiple
		 * replies per message. The same reply object can be used to receive
		 * multiple times.
		 */
		while (operations_done < operation_count)
		{
			guint32 reply_operation_count;

			j_message_receive(reply, object_connection);

			reply_operation_count = j_message_get_count(reply);

			for (guint i = 0; i < reply_operation_count && j_list_iterator_next(it); i++)
			{
				JTransformationObjectOperation* operation = j_list_iterator_get(it);
				gpointer data = operation->read.data;
				guint64* bytes_read = operation->read.bytes_read;

				guint64 nbytes;

				nbytes = j_message_get_8(reply);
				j_helper_atomic_add(bytes_read, nbytes);

				if (nbytes > 0)
				{
					GInputStream* input;

					input = g_io_stream_get_input_stream(G_IO_STREAM(object_connection));
					g_input_stream_read_all(input, data, nbytes, NULL, NULL, NULL);
				}

                // Transform the read data if the object has a transformation set
                if(transformation != NULL)
                {
                    j_transformation_apply(transformation, &data, &operation->read.length,
						&operation->read.offset, J_TRANSFORMATION_CALLER_CLIENT_READ);
					// data, length, offset must not change here
                }
			}

			operations_done += reply_operation_count;
		}

		j_list_iterator_free(it);

		j_connection_pool_push_object(object->index, object_connection);
	}

	/*
	if (lock != NULL)
	{
		// FIXME busy wait
		while (!j_lock_acquire(lock));

		j_lock_free(lock);
	}
	*/

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
j_transformation_object_write_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = TRUE;

	JBackend* object_backend;
	JListIterator* it;
	g_autoptr(JMessage) message = NULL;
	JObject* object;
    JTransformationObject* transformation_object;
	gpointer object_handle;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JTransformationObjectOperation* operation = j_list_get_first(operations);

		transformation_object = operation->status.object;
        object = transformation_object->object;

		g_assert(operation != NULL);
		g_assert(object != NULL);
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_backend();

	if (object_backend != NULL)
	{
		ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
	}
	else
	{
		gsize name_len;
		gsize namespace_len;

		namespace_len = strlen(object->namespace) + 1;
		name_len = strlen(object->name) + 1;

		message = j_message_new(J_MESSAGE_OBJECT_WRITE, namespace_len + name_len);
		j_message_set_safety(message, semantics);
		j_message_append_n(message, object->namespace, namespace_len);
		j_message_append_n(message, object->name, name_len);
	}

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("item", path);
	}
	*/

	while (j_list_iterator_next(it))
	{
		JTransformationObjectOperation* operation = j_list_iterator_get(it);
		gconstpointer data = operation->write.data;
		guint64 length = operation->write.length;
		guint64 offset = operation->write.offset;
		guint64* bytes_written = operation->write.bytes_written;

        JTransformation* transformation = transformation_object->transformation;

		j_trace_file_begin(object->name, J_TRACE_FILE_WRITE);

		/*
		if (lock != NULL)
		{
			j_lock_add(lock, block_id);
		}
		*/

        //Transform the data if necessary
        if(transformation != NULL)
        {
            j_transformation_apply(transformation, &data, &length, &offset,
				J_TRANSFORMATION_CALLER_CLIENT_WRITE);
			// data, length, offset could have changed
            operation->write.data = data;
			operation->write.length = length;
			operation->write.offset = offset;
        }


		if (object_backend != NULL)
		{
			guint64 nbytes = 0;

			ret = j_backend_object_write(object_backend, object_handle, data, length, offset, &nbytes) && ret;
			j_helper_atomic_add(bytes_written, nbytes);
		}
		else
		{
			j_message_add_operation(message, sizeof(guint64) + sizeof(guint64));
			j_message_append_8(message, &length);
			j_message_append_8(message, &offset);
			j_message_add_send(message, data, length);

			// Fake bytes_written here instead of doing another loop further down
			if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) == J_SEMANTICS_SAFETY_NONE)
			{
				j_helper_atomic_add(bytes_written, length);
			}
		}

		j_trace_file_end(object->name, J_TRACE_FILE_WRITE, length, offset);
	}

	j_list_iterator_free(it);

	if (object_backend != NULL)
	{
		ret = j_backend_object_close(object_backend, object_handle) && ret;
	}
	else
	{
		GSocketConnection* object_connection;

		object_connection = j_connection_pool_pop_object(object->index);
		j_message_send(message, object_connection);

		if (j_message_get_flags(message) & J_MESSAGE_FLAGS_SAFETY_NETWORK)
		{
			g_autoptr(JMessage) reply = NULL;
			guint64 nbytes;

			reply = j_message_new_reply(message);
			j_message_receive(reply, object_connection);

			it = j_list_iterator_new(operations);

			while (j_list_iterator_next(it))
			{
				JTransformationObjectOperation* operation = j_list_iterator_get(it);
				guint64* bytes_written = operation->write.bytes_written;

				nbytes = j_message_get_8(reply);
				j_helper_atomic_add(bytes_written, nbytes);
			}

			j_list_iterator_free(it);
		}

		j_connection_pool_push_object(object->index, object_connection);
	}

	/*
	if (lock != NULL)
	{
		// FIXME busy wait
		while (!j_lock_acquire(lock));

		j_lock_free(lock);
	}
	*/

	j_trace_leave(G_STRFUNC);

	return ret;
}
/**
 * Creates a new item.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * JTransformationObject* i;
 *
 * i = j_transformation_object_new("JULEA");
 * \endcode
 *
 * \param name         An item name.
 * \param distribution A distribution.
 *
 * \return A new item. Should be freed with j_transformation_object_unref().
 **/
JTransformationObject*
j_transformation_object_new (gchar const* namespace, gchar const* name, JTransformationType transformation_type, JTransformationMode transformation_mode)
{
    JTransformationObject* object;

    j_trace_enter(G_STRFUNC, NULL);

    object = g_slice_new(JTransformationObject);
    object->object = j_object_new(namespace, name);
    object->ref_count = 1;
    object->transformation = j_transformation_new(transformation_type, transformation_mode, NULL);
    
    j_trace_leave(G_STRFUNC);

    return object;
}

/**
 * Creates a new item.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * JTransformationObject* i;
 *
 * i = j_transformation_object_new("JULEA");
 * \endcode
 *
 * \param name         An item name.
 * \param distribution A distribution.
 *
 * \return A new item. Should be freed with j_transformation_object_unref().
 **/
JTransformationObject*
j_transformation_object_new_for_index (guint32 index, gchar const* namespace, gchar const* name, JTransformationType transformation_type, JTransformationMode transformation_mode)
{
    JTransformationObject* object;

	j_trace_enter(G_STRFUNC, NULL);

	object = g_slice_new(JTransformationObject);
    object->object = j_object_new_for_index (index, namespace, name);
	object->ref_count = 1;
    object->transformation = j_transformation_new(transformation_type, transformation_mode, NULL);

	j_trace_leave(G_STRFUNC);

	return object;
}

/**
 * Increases an item's reference count.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * JTransformationObject* i;
 *
 * j_transformation_object_ref(i);
 * \endcode
 *
 * \param item An item.
 *
 * \return #item.
 **/
JTransformationObject*
j_transformation_object_ref (JTransformationObject* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	g_atomic_int_inc(&(item->ref_count));

	j_trace_leave(G_STRFUNC);

	return item;
}

/**
 * Decreases an item's reference count.
 * When the reference count reaches zero, frees the memory allocated for the item.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param item An item.
 **/
void
j_transformation_object_unref (JTransformationObject* item)
{
	g_return_if_fail(item != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if (g_atomic_int_dec_and_test(&(item->ref_count)))
	{
        j_object_unref(item->object);

		g_slice_free(JTransformationObject, item);
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Creates an object.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param name         A name.
 * \param distribution A distribution.
 * \param batch        A batch.
 *
 * \return A new item. Should be freed with j_transformation_object_unref().
 **/
void
j_transformation_object_create (JTransformationObject* object, JBatch* batch)
{
    j_object_create(object->object, batch);
}

/**
 * Deletes an object.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param item       An item.
 * \param batch      A batch.
 **/
void
j_transformation_object_delete (JTransformationObject* object, JBatch* batch)
{
    j_object_delete(object->object, batch);
}

/**
 * Reads an item.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param object     An object.
 * \param data       A buffer to hold the read data.
 * \param length     Number of bytes to read.
 * \param offset     An offset within #object.
 * \param bytes_read Number of bytes read.
 * \param batch      A batch.
 **/
void
j_transformation_object_read (JTransformationObject* object, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch)
{
	JTransformationObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);
	g_return_if_fail(bytes_read != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	iop = g_slice_new(JTransformationObjectOperation);
	iop->read.object = j_transformation_object_ref(object);
	iop->read.data = data;
	iop->read.length = length;
	iop->read.offset = offset;
	iop->read.bytes_read = bytes_read;

	operation = j_operation_new();
	operation->key = object;
	operation->data = iop;
	operation->exec_func = j_transformation_object_read_exec;
	operation->free_func = j_transformation_object_read_free;

	*bytes_read = 0;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
}

/**
 * Writes an item.
 *
 * \note
 * j_transformation_object_write() modifies bytes_written even if j_batch_execute() is not called.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param item          An item.
 * \param data          A buffer holding the data to write.
 * \param length        Number of bytes to write.
 * \param offset        An offset within #item.
 * \param bytes_written Number of bytes written.
 * \param batch         A batch.
 **/
void
j_transformation_object_write (JTransformationObject* object, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch)
{
	JTransformationObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);
	g_return_if_fail(bytes_written != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	iop = g_slice_new(JTransformationObjectOperation);
	iop->write.object = j_transformation_object_ref(object);
	iop->write.data = data;
	iop->write.length = length;
	iop->write.offset = offset;
	iop->write.bytes_written = bytes_written;

	operation = j_operation_new();
	operation->key = object;
	operation->data = iop;
	operation->exec_func = j_transformation_object_write_exec;
	operation->free_func = j_transformation_object_write_free;

	*bytes_written = 0;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
}


/**
 * Get the status of an item.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param item      An item.
 * \param batch     A batch.
 **/
void
j_transformation_object_status (JTransformationObject* object, gint64* modification_time, guint64* size, JBatch* batch)
{
    j_object_status(object->object, modification_time, size, batch);
}

/**
 * @}
 **/
