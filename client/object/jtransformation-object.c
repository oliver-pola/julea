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

#include <julea-kv.h>

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
			guint64* original_size;
			guint64* transformed_size;
			JTransformationType* transformation_type;
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

    /**
     * KV Object which stores transformation metadata
     **/
    JKV* metadata;

    /**
     * The size of the object in its detransformed state
     **/
    guint64 original_size;

    /**
     * The size of the object in its transformed state
     **/
    guint64 transformed_size;
};

static
void
j_transformation_object_create_free (gpointer data)
{
	JTransformationObject* object = data;

	j_transformation_object_unref(object);
}

static
void
j_transformation_object_delete_free (gpointer data)
{
	JTransformationObject* object = data;

	j_transformation_object_unref(object);
}

static
void
j_transformation_object_status_free (gpointer data)
{
	JTransformationObjectOperation* operation = data;

	j_transformation_object_unref(operation->status.object);

	g_slice_free(JTransformationObjectOperation, operation);
}

static
void
j_transformation_object_read_free (gpointer data)
{
	JTransformationObjectOperation* operation = data;

	j_transformation_object_unref(operation->read.object);

	g_slice_free(JTransformationObjectOperation, operation);
}

static
void
j_transformation_object_write_free (gpointer data)
{
	JTransformationObjectOperation* operation = data;

	j_transformation_cleanup(operation->write.object->transformation,
		operation->write.data, operation->write.length, operation->write.offset,
		J_TRANSFORMATION_CALLER_CLIENT_WRITE);
	g_slice_free(JTransformationObjectOperation, operation);

	j_transformation_object_unref(operation->write.object);
}

static
gboolean
j_transformation_object_create_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* object_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autoptr(JMessage) message = NULL;
	gchar const* namespace;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JTransformationObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_backend();

	if (object_backend == NULL)
	{
		/**
		 * Force safe semantics to make the server send a reply.
		 * Otherwise, nasty races can occur when using unsafe semantics:
		 * - The client creates the item and sends its first write.
		 * - The client sends another operation using another connection from the pool.
		 * - The second operation is executed first and fails because the item does not exist.
		 * This does not completely eliminate all races but fixes the common case of create, write, write, ...
		 **/
		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_CREATE, namespace_len);
		j_message_set_safety(message, semantics);
		//j_message_force_safety(message, J_SEMANTICS_SAFETY_NETWORK);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JTransformationObject* object = j_list_iterator_get(it);

		if (object_backend != NULL)
		{
			gpointer object_handle;

			ret = j_backend_object_create(object_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_object_close(object_backend, object_handle) && ret;
		}
		else
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			j_message_add_operation(message, name_len);
			j_message_append_n(message, object->name, name_len);
		}
	}

	if (object_backend == NULL)
	{
		GSocketConnection* object_connection;

		object_connection = j_connection_pool_pop_object(index);
		j_message_send(message, object_connection);

		if (j_message_get_flags(message) & J_MESSAGE_FLAGS_SAFETY_NETWORK)
		{
			g_autoptr(JMessage) reply = NULL;

			reply = j_message_new_reply(message);
			j_message_receive(reply, object_connection);

			/* FIXME do something with reply */
		}

		j_connection_pool_push_object(index, object_connection);
	}

	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gboolean
j_transformation_object_delete_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* object_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autoptr(JMessage) message = NULL;
	gchar const* namespace;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JTransformationObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;

        // Also delete the metadata from the KV store
        JBatch* kv_batch = j_batch_new(semantics);
        j_kv_delete(object->metadata, kv_batch);
        j_batch_execute(kv_batch);
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_backend();

	if (object_backend == NULL)
	{
		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_DELETE, namespace_len);
		j_message_set_safety(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JTransformationObject* object = j_list_iterator_get(it);

		if (object_backend != NULL)
		{
			gpointer object_handle;

			ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_object_delete(object_backend, object_handle) && ret;
		}
		else
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			j_message_add_operation(message, name_len);
			j_message_append_n(message, object->name, name_len);
		}
	}

	if (object_backend == NULL)
	{
		GSocketConnection* object_connection;

		object_connection = j_connection_pool_pop_object(index);
		j_message_send(message, object_connection);

		if (j_message_get_flags(message) & J_MESSAGE_FLAGS_SAFETY_NETWORK)
		{
			g_autoptr(JMessage) reply = NULL;

			reply = j_message_new_reply(message);
			j_message_receive(reply, object_connection);

			/* FIXME do something with reply */
		}

		j_connection_pool_push_object(index, object_connection);
	}


	j_trace_leave(G_STRFUNC);

	return ret;
}

static
void
j_transformation_object_set_transformation(JTransformationObject* object, JTransformationType type, JTransformationMode mode, void* params)
{
    object->transformation = j_transformation_new(type, mode, params);
}


static
bool
j_transformation_object_load_transformation(JTransformationObject* object, JSemantics* semantics)
{
    bool ret = false;
    // TODO memoty management pointer
    bson_t* metadata_bson = bson_new();
    JBatch* kv_batch = j_batch_new(semantics);

    j_kv_get(object->metadata, metadata_bson, kv_batch);
    j_batch_execute(kv_batch);

    bson_iter_t iter;
    if(bson_iter_init(&iter, metadata_bson))
    {
        bson_iter_find(&iter, "JTransformationType");
        int type = bson_iter_int32(&iter);
        bson_iter_find(&iter, "JTransformationMode");
        int mode = bson_iter_int32(&iter);
        bson_iter_find(&iter, "original_size");
        guint64 original_size = bson_iter_int64(&iter);
        bson_iter_find(&iter, "transformed_size");
        guint64 transformed_size = bson_iter_int64(&iter);

        // TODO handle params struct
        j_transformation_object_set_transformation(object, type, mode, NULL);
        object->original_size = original_size;
        object->transformed_size = original_size;
        ret = true;
    }
    return ret;
}

static
bool
j_transformation_object_load_object_size(JTransformationObject* object, JSemantics* semantics)
{
    bool ret = false;
    // TODO memoty management pointer
    bson_t* metadata_bson = bson_new();
    JBatch* kv_batch = j_batch_new(semantics);

    j_kv_get(object->metadata, metadata_bson, kv_batch);
    j_batch_execute(kv_batch);

    bson_iter_t iter;
    if(bson_iter_init(&iter, metadata_bson))
    {
        bson_iter_find(&iter, "original_size");
        guint64 original_size = bson_iter_int64(&iter);
        bson_iter_find(&iter, "transformed_size");
        guint64 transformed_size = bson_iter_int64(&iter);

        object->original_size = original_size;
        object->transformed_size = transformed_size;
        ret = true;
    }
    return ret;
}

static
void
j_transformation_object_update_object_size(JTransformationObject* object, JSemantics* semantics)
{
    JBatch* kv_batch = j_batch_new(semantics);

    bson_t* metadata_bson = bson_new();
    bson_append_int32(metadata_bson, "JTransformationType", -1, (int)j_transformation_get_type(object->transformation));
    bson_append_int32(metadata_bson, "JTransformationMode", -1, (int)j_transformation_get_mode(object->transformation));
    bson_append_int64(metadata_bson, "original_size", -1, object->original_size);
    bson_append_int64(metadata_bson, "transformed_size", -1, object->transformed_size);

    j_kv_put(object->metadata, metadata_bson, kv_batch);
    j_batch_execute(kv_batch);
}

static
gboolean
j_transformation_object_read_all (JTransformationObject* object,
	gpointer* data, guint64* length, JSemantics* semantics)
{
	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autoptr(JMessage) message = NULL;
	gpointer object_handle;

	// get object size with separate message in case without object backend
	g_autoptr(JMessage) message_status = NULL;
	guint64 object_size;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(object != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);


	object_backend = j_object_backend();

	if (object_backend != NULL)
	{
		ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
		// Get the object size, see procedure in _status_exec()
		ret = j_backend_object_status(object_backend, object_handle, NULL, &object_size) && ret;
	}
	else
	{
		gsize name_len;
		gsize namespace_len;
		g_autoptr(JMessage) reply = NULL;
		GSocketConnection* object_connection;

		// Always need the stored data as it is, type ierrelevant here.
		// Sending the type was just meant to make it easier for the server
		// in _MODE_SERVER when the transformation client already knows that
		guint8 transformation_mode = J_TRANSFORMATION_MODE_CLIENT;
		guint8 transformation_type = J_TRANSFORMATION_TYPE_NONE;

		namespace_len = strlen(object->namespace) + 1;
		name_len = strlen(object->name) + 1;

		// Prepare the message for data
		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_READ, 2 + namespace_len + name_len);
		j_message_set_safety(message, semantics);
		j_message_append_1(message, &transformation_mode);
		j_message_append_1(message, &transformation_type);
		j_message_append_n(message, object->namespace, namespace_len);
		j_message_append_n(message, object->name, name_len);

		// Get the object size, see procedure in _status_exec()
		// We need the replied object_size to allocate memory
		// and further construct the data message
		message_status = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_STATUS, namespace_len);
		j_message_set_safety(message_status, semantics);
		j_message_append_n(message_status, object->namespace, namespace_len);

		j_message_add_operation(message_status, name_len);
		j_message_append_n(message_status, object->name, name_len);

		object_connection = j_connection_pool_pop_object(object->index);
		j_message_send(message_status, object_connection);

		reply = j_message_new_reply(message_status);
		j_message_receive(reply, object_connection);

		// gint64 modification_time; // recieve and ignore
		// modification_time =
		j_message_get_8(reply);
		object_size = j_message_get_8(reply);

		j_connection_pool_push_object(object->index, object_connection);
	}


    object_size = object->transformed_size;
	*length = object_size;

	if (object_size == 0)
	{
		*data = NULL;
		return TRUE;
	}

	// allocate memory for whole object
	*data = g_slice_alloc(object_size);

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("item", path);
	}
	*/

	j_trace_file_begin(object->name, J_TRACE_FILE_READ);

	if (object_backend != NULL)
	{
		guint64 nbytes = 0;
		ret = j_backend_object_read(object_backend, object_handle, *data, *length, 0, &nbytes) && ret;
	}
	else
	{
		guint64 offset = 0;
		j_message_add_operation(message, sizeof(guint64) + sizeof(guint64));
		j_message_append_8(message, length);
		j_message_append_8(message, &offset);
	}

	j_trace_file_end(object->name, J_TRACE_FILE_READ, *length, 0);

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

			for (guint i = 0; i < reply_operation_count; i++)
			{
				guint64 nbytes;
				nbytes = j_message_get_8(reply);

				if (nbytes > 0)
				{
					GInputStream* input;

					input = g_io_stream_get_input_stream(G_IO_STREAM(object_connection));
					g_input_stream_read_all(input, *data, nbytes, NULL, NULL, NULL);
				}

			}
			operations_done += reply_operation_count;
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

static
gboolean
j_transformation_object_read_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = TRUE;

	JBackend* object_backend;
	JListIterator* it;
	g_autoptr(JMessage) message = NULL;
    JTransformationObject* object;
	gpointer object_handle;
    JTransformation* transformation = NULL;
	// for complex transformations we will need to load the whole object
	guint8* object_data;
	guint64 object_size;
	gboolean transformation_need_whole_object = FALSE;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JTransformationObjectOperation* operation = j_list_get_first(operations);

        object = operation->read.object;

        transformation = object->transformation;
        if(transformation == NULL)
        {
            j_transformation_object_load_transformation(object, semantics);
            transformation = object->transformation;
        }
        // If a transformation is already set we need to get the current object sizes from the KV store
        else
        {
            j_transformation_object_load_object_size(object, semantics);
        }

		g_assert(operation != NULL);
		g_assert(object != NULL);
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_backend();

	transformation_need_whole_object = j_transformation_need_whole_object(transformation,
		J_TRANSFORMATION_CALLER_CLIENT_READ);

	if (transformation_need_whole_object)
	{
		guint64 buflength, bufoffset;
		gpointer buffer = NULL;
		j_transformation_object_read_all(object, (gpointer*)&object_data, &object_size, semantics);
		if (object_size > 0)
		{
			// well, usually client read already has memory allocated that fits the request
			// but here we don't know the whole size after transformation
			// TODO as a workaround convention _apply() will hand out the just created buffer
			// that perfectly fits, if we provide NULL as output buffer
			j_transformation_apply(transformation, object_data, object_size, 0,
				&buffer, &buflength, &bufoffset, J_TRANSFORMATION_CALLER_CLIENT_READ);
			// now we have the original data in buffer and don't need the recieved version anymore
			g_slice_free1(object_size, object_data);
			object_data = buffer;
			object_size = buflength;
		}
	}
	else if (object_backend != NULL)
	{
		ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
	}
	else
	{
		gsize name_len;
		gsize namespace_len;

		guint8 transformation_mode = (guint8)j_transformation_get_mode(transformation);
		guint8 transformation_type = (guint8)j_transformation_get_type(transformation);

		namespace_len = strlen(object->namespace) + 1;
		name_len = strlen(object->name) + 1;

		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_READ, 2 + namespace_len + name_len);
		j_message_set_safety(message, semantics);
		j_message_append_1(message, &transformation_mode);
		j_message_append_1(message, &transformation_type);
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

		j_trace_file_begin(object->name, J_TRACE_FILE_READ);

		if (transformation_need_whole_object)
		{
			// Apply read operations in-memory
			guint64 length_to_eof;
			if (offset > object_size)
				length_to_eof = 0; // avoid negatives, it's unsigned!
			else
				length_to_eof = object_size - offset;
			length = MIN(length, length_to_eof);
			if (length > 0)
			{
				memcpy(data, object_data + offset, length);
				j_helper_atomic_add(bytes_read, length);
			}
            // DEBUG printf("READ: orig_size: %ld, transformed_size: %ld\n", object->original_size, object->transformed_size);
		}
		else if (object_backend != NULL)
		{
			guint64 nbytes = 0;

			ret = j_backend_object_read(object_backend, object_handle, data, length, offset, &nbytes) && ret;
			j_helper_atomic_add(bytes_read, nbytes);

            // Transform the read data if the object has a transformation set
            j_transformation_apply(transformation, data, length, offset,
				&data, &length, &offset, J_TRANSFORMATION_CALLER_CLIENT_READ);
			j_transformation_cleanup(transformation, data, length, offset,
				J_TRANSFORMATION_CALLER_CLIENT_READ);
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

	if (transformation_need_whole_object)
	{
		// Cleanup of whole in-memory object
		g_slice_free1(object_size, object_data);
	}
	else if (object_backend != NULL)
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
				guint64 length = operation->read.length;
				guint64 offset = operation->read.offset;
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
                j_transformation_apply(transformation, data, length, offset,
					&data, &length, &offset,
					J_TRANSFORMATION_CALLER_CLIENT_READ);
				j_transformation_cleanup(transformation, data, length, offset,
					J_TRANSFORMATION_CALLER_CLIENT_READ);
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
j_transformation_object_write_all (JTransformationObject* object,
	gpointer data, guint64 length, JSemantics* semantics)
{
	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autoptr(JMessage) message = NULL;
	gpointer object_handle;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(object != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	object_backend = j_object_backend();

	if (object_backend != NULL)
	{
		ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
	}
	else
	{
		gsize name_len;
		gsize namespace_len;

		// Always write data to storage as it is, type ierrelevant here.
		// Sending the type was just meant to make it easier for the server
		// in _MODE_SERVER when the transformation client already knows that
		guint8 transformation_mode = J_TRANSFORMATION_MODE_CLIENT;
		guint8 transformation_type = J_TRANSFORMATION_TYPE_NONE;

		namespace_len = strlen(object->namespace) + 1;
		name_len = strlen(object->name) + 1;

		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_WRITE, 2 + namespace_len + name_len);
		j_message_set_safety(message, semantics);
		j_message_append_1(message, &transformation_mode);
		j_message_append_1(message, &transformation_type);
		j_message_append_n(message, object->namespace, namespace_len);
		j_message_append_n(message, object->name, name_len);
	}

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("item", path);
	}
	*/

	j_trace_file_begin(object->name, J_TRACE_FILE_WRITE);

	/*
	if (lock != NULL)
	{
		j_lock_add(lock, block_id);
	}
	*/

	if (object_backend != NULL)
	{
		guint64 nbytes = 0;
		ret = j_backend_object_write(object_backend, object_handle, data, length, 0, &nbytes) && ret;
	}
	else
	{
		guint64 offset = 0;
		j_message_add_operation(message, sizeof(guint64) + sizeof(guint64));
		j_message_append_8(message, &length);
		j_message_append_8(message, &offset);
		j_message_add_send(message, data, length);
	}

	j_trace_file_end(object->name, J_TRACE_FILE_WRITE, length, 0);

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
			// guint64 nbytes;

			reply = j_message_new_reply(message);
			j_message_receive(reply, object_connection);

			// nbytes =
			j_message_get_8(reply);
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

static
gboolean
j_transformation_object_write_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = TRUE;

	JBackend* object_backend;
	JListIterator* it;
	g_autoptr(JMessage) message = NULL;
    JTransformationObject* object;
	gpointer object_handle;
	JTransformation* transformation = NULL;
	// for complex transformations we will need to load the whole object
	guint8* object_data;
	guint64 object_size;
	gboolean transformation_need_whole_object = FALSE;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JTransformationObjectOperation* operation = j_list_get_first(operations);

		object = operation->write.object;

		transformation = object->transformation;
        if(transformation == NULL)
        {
            j_transformation_object_load_transformation(object, semantics);
            transformation = object->transformation;
        }
        else
        {
            j_transformation_object_load_object_size(object, semantics);
        }

		g_assert(operation != NULL);
		g_assert(object != NULL);
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_backend();

	transformation_need_whole_object = j_transformation_need_whole_object(transformation,
		J_TRANSFORMATION_CALLER_CLIENT_READ);

	if (transformation_need_whole_object)
	{
		guint64 object_grown_size;
		guint64 buflength, bufoffset;
		guint8* buffer = NULL;

		j_transformation_object_read_all(object, (gpointer*)&object_data, &object_size, semantics);
		if (object_size > 0)
		{
			// Watch out: This is a _CLIENT_READ even if we are inside _write_exec()
			// well, usually client read already has memory allocated that fits the request
			// but here we don't know the whole size after transformation
			// TODO as a workaround convention _apply() will hand out the just created buffer
			// that perfectly fits, if we provide NULL as output buffer
			j_transformation_apply(transformation, object_data, object_size, 0,
				(gpointer*)&buffer, &buflength, &bufoffset, J_TRANSFORMATION_CALLER_CLIENT_READ);
			// now we have the original data in buffer and don't need the recieved version anymore
			g_slice_free1(object_size, object_data);
			object_data = buffer;
			object_size = buflength;
		}

		// prepare for growing object
		object_grown_size = object_size;
		while (j_list_iterator_next(it))
		{
			JTransformationObjectOperation* operation = j_list_iterator_get(it);
			object_grown_size = MAX(object_grown_size,
				operation->write.offset + operation->write.length);
		}
		j_list_iterator_free(it);
		it = j_list_iterator_new(operations);
		// get new memory in proper size
		if (object_grown_size > object_size)
		{
			buffer = g_slice_alloc(object_grown_size);
			memcpy(buffer, object_data, object_size);
			memset(buffer + object_size, 0, object_grown_size - object_size);
			g_slice_free1(object_size, object_data);
			object_data = buffer;
			object_size = object_grown_size;
		}
	}
	else if (object_backend != NULL)
	{
		ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
	}
	else
	{
		gsize name_len;
		gsize namespace_len;

		guint8 transformation_mode = (guint8)j_transformation_get_mode(transformation);
		guint8 transformation_type = (guint8)j_transformation_get_type(transformation);

		namespace_len = strlen(object->namespace) + 1;
		name_len = strlen(object->name) + 1;

		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_WRITE, 2 + namespace_len + name_len);
		j_message_set_safety(message, semantics);
		j_message_append_1(message, &transformation_mode);
		j_message_append_1(message, &transformation_type);
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

		j_trace_file_begin(object->name, J_TRACE_FILE_WRITE);

		/*
		if (lock != NULL)
		{
			j_lock_add(lock, block_id);
		}
		*/

		if (transformation_need_whole_object)
		{
			// Apply read operations in-memory
			// boundaries already checked
			memcpy(object_data + offset, data, length);
			j_helper_atomic_add(bytes_written, length);
		}
		else if (object_backend != NULL)
		{
			guint64 nbytes = 0;

			// this is the simple case where length and offset do not change
            j_transformation_apply(transformation, data, length, offset,
				&data, &length, &offset, J_TRANSFORMATION_CALLER_CLIENT_WRITE);
			// data changed to new buffer, stored in operation to free later
            operation->write.data = data;

			ret = j_backend_object_write(object_backend, object_handle, data, length, offset, &nbytes) && ret;
			j_helper_atomic_add(bytes_written, nbytes);
		}
		else
		{
			// this is the simple case where length and offset do not change
            j_transformation_apply(transformation, data, length, offset,
				&data, &length, &offset, J_TRANSFORMATION_CALLER_CLIENT_WRITE);
			// data changed to new buffer, stored in operation to free later
            operation->write.data = data;

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

	if (transformation_need_whole_object)
	{
		guint64 buflength, bufoffset;
		gpointer buffer = NULL;
		j_transformation_apply(transformation, object_data, object_size, 0,
			&buffer, &buflength, &bufoffset, J_TRANSFORMATION_CALLER_CLIENT_WRITE);

		j_transformation_object_write_all (object, buffer, buflength, semantics);
        //ADDED
        object->original_size = object_size;
        object->transformed_size = buflength;
        j_transformation_object_update_object_size(object, semantics);
        // DEBUG printf("Write: orig_size: %ld, transformed_size: %ld\n", object->original_size, object->transformed_size);

		// Cleanup of whole in-memory object
		g_slice_free1(object_size, object_data);
		g_slice_free1(buflength, buffer);
	}
	else if (object_backend != NULL)
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

static
gboolean
j_transformation_object_status_exec (JList* operations, JSemantics* semantics)
{
	gboolean ret = FALSE;

	JBackend* object_backend;
	JListIterator* it;
	g_autoptr(JMessage) message = NULL;
	gchar const* namespace;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	{
		JTransformationObjectOperation* operation = j_list_get_first(operations);
		JTransformationObject* object = operation->status.object;

		g_assert(operation != NULL);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_backend();

	if (object_backend == NULL)
	{
		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_STATUS, namespace_len);
		j_message_set_safety(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JTransformationObjectOperation* operation = j_list_iterator_get(it);
		JTransformationObject* object = operation->status.object;
		gint64* modification_time = operation->status.modification_time;
		guint64* original_size = operation->status.original_size;
		guint64* transformed_size = operation->status.transformed_size;
		JTransformationType* transformation_type = operation->status.transformation_type;

		if (object_backend != NULL)
		{
			gpointer object_handle;
			// The size we could get from status is rather a physical size
			// transformed < original < physical is possible
			// original < transformed < physical is also possible
			guint64 size;

			ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_object_status(object_backend, object_handle, modification_time, &size) && ret;
			ret = j_backend_object_close(object_backend, object_handle) && ret;

			// We have the sizes in JTransformationObject already
			if (original_size != NULL)
			{
				*original_size = object->original_size;
			}
			if (transformed_size != NULL)
			{
				*transformed_size = object->transformed_size;
			}
			if (transformation_type != NULL)
			{
				*transformation_type = j_transformation_get_type(object->transformation);
			}
		}
		else
		{
			gsize name_len;

			name_len = strlen(object->name) + 1;

			j_message_add_operation(message, name_len);
			j_message_append_n(message, object->name, name_len);
		}
	}

	j_list_iterator_free(it);

	if (object_backend == NULL)
	{
		g_autoptr(JMessage) reply = NULL;
		GSocketConnection* object_connection;

		object_connection = j_connection_pool_pop_object(index);
		j_message_send(message, object_connection);

		reply = j_message_new_reply(message);
		j_message_receive(reply, object_connection);

		it = j_list_iterator_new(operations);

		while (j_list_iterator_next(it))
		{
			JTransformationObjectOperation* operation = j_list_iterator_get(it);
			gint64* modification_time = operation->status.modification_time;
			guint64* original_size = operation->status.original_size;
			guint64* transformed_size = operation->status.transformed_size;
			JTransformationType* transformation_type = operation->status.transformation_type;
			gint64 modification_time_;
			guint64 original_size_;
			guint64 transformed_size_;
			JTransformationType transformation_type_;

			modification_time_ = j_message_get_8(reply);
			original_size_ = j_message_get_8(reply);
			transformed_size_ = j_message_get_8(reply);
			transformation_type_ = j_message_get_1(reply);

			if (modification_time != NULL)
			{
				*modification_time = modification_time_;
			}
			// TODO if we ask server for mtime, we should also use his sizes
			// there is a TODO in server.c
			// Using data from JTransformationObject in the meanwhile
			if (original_size != NULL)
			{
				// TODO *original_size = original_size_;
				*original_size = operation->status.object->original_size;
			}
			if (transformed_size != NULL)
			{
				// TODO *transformed_size = transformed_size_;
				*transformed_size = operation->status.object->transformed_size;
			}
			if (transformation_type != NULL)
			{
				// TODO *transformation_type = transformation_type_;
				*transformation_type = j_transformation_get_type(operation->status.object->transformation);
			}
		}

		j_list_iterator_free(it);

		j_connection_pool_push_object(index, object_connection);
	}

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
j_transformation_object_new (gchar const* namespace, gchar const* name)
{
	JConfiguration* configuration = j_configuration();
	JTransformationObject* object;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	object = g_slice_new(JTransformationObject);
	object->index = j_helper_hash(name) % j_configuration_get_object_server_count(configuration);
	object->namespace = g_strdup(namespace);
	object->name = g_strdup(name);
	object->ref_count = 1;
    object->metadata = j_kv_new(namespace, name);
    object->original_size = 0;
    object->transformed_size = 0;

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
j_transformation_object_new_for_index (guint32 index, gchar const* namespace, gchar const* name)
{
	JConfiguration* configuration = j_configuration();
	JTransformationObject* object;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);
	g_return_val_if_fail(index < j_configuration_get_object_server_count(configuration), NULL);

	j_trace_enter(G_STRFUNC, NULL);

	object = g_slice_new(JTransformationObject);
	object->index = index;
	object->namespace = g_strdup(namespace);
	object->name = g_strdup(name);
	object->ref_count = 1;
    object->metadata = j_kv_new(namespace, name);

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
		g_free(item->name);
		g_free(item->namespace);

		if (item->transformation)
			j_transformation_unref(item->transformation);

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
j_transformation_object_create (JTransformationObject* object, JBatch* batch, JTransformationType type, JTransformationMode mode, void* params)
{
	JOperation* operation;

	g_return_if_fail(object != NULL);

	j_trace_enter(G_STRFUNC, NULL);

    // Set the transformation
    j_transformation_object_set_transformation(object, type, mode, params);

    // Store transformation information on the KV server
    // TODO delete

    JBatch* kv_batch = j_batch_new(j_batch_get_semantics(batch));

    bson_t* metadata_bson = bson_new();
    bson_append_int32(metadata_bson, "JTransformationType", -1, (int)type);
    bson_append_int32(metadata_bson, "JTransformationMode", -1, (int)mode);
    bson_append_int64(metadata_bson, "original_size", -1, 0);
    bson_append_int64(metadata_bson, "transformed_size", -1, 0);
    // TODO handle params struct

    j_kv_put(object->metadata, metadata_bson, kv_batch);
    j_batch_execute(kv_batch);


	operation = j_operation_new();
	// FIXME key = index + namespace
	operation->key = object;
	operation->data = j_transformation_object_ref(object);
	operation->exec_func = j_transformation_object_create_exec;
	operation->free_func = j_transformation_object_create_free;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
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
	JOperation* operation;

	g_return_if_fail(object != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	operation = j_operation_new();
	operation->key = object;
	operation->data = j_transformation_object_ref(object);
	operation->exec_func = j_transformation_object_delete_exec;
	operation->free_func = j_transformation_object_delete_free;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
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
j_transformation_object_status (JTransformationObject* object, gint64* modification_time,
	guint64* size, JBatch* batch)
{
	j_transformation_object_status_ext(object, modification_time, size, NULL, NULL, batch);
}

/**
 * Get the status of an item with transformation properties.
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
j_transformation_object_status_ext (JTransformationObject* object, gint64* modification_time,
	guint64* original_size, guint64* transformed_size, JTransformationType* transformation_type, JBatch* batch)
{
	JTransformationObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	iop = g_slice_new(JTransformationObjectOperation);
	iop->status.object = j_transformation_object_ref(object);
	iop->status.modification_time = modification_time;
	iop->status.original_size = original_size;
	iop->status.transformed_size = transformed_size;
	iop->status.transformation_type = transformation_type;

	operation = j_operation_new();
	operation->key = object;
	operation->data = iop;
	operation->exec_func = j_transformation_object_status_exec;
	operation->free_func = j_transformation_object_status_free;

	j_batch_add(batch, operation);

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
