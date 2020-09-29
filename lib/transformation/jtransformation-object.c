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

#include <transformation/jtransformation-object.h>
#include <julea-object.h>
#include <julea-kv.h>
#include <julea.h>

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
		} status;

		struct
		{
			JTransformationObject* object;
			gpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_read;
		} read;

		struct
		{
			JTransformationObject* object;
			gpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_written;
		} write;
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

/**
 * Metadata fields needed for object management. 
 * The metadata for each object will be in the kv-store
 **/
struct JTransformationObjectMetadata
{
	gint32 transformation_type;
	gint32 transformation_mode;
	guint64 original_size;
	guint64 transformed_size;
};

typedef struct JTransformationObjectMetadata JTransformationObjectMetadata;

static JBackend* j_object_backend = NULL;
static GModule* j_object_module = NULL;

// FIXME copy and use GLib's G_DEFINE_CONSTRUCTOR/DESTRUCTOR
static void __attribute__((constructor)) j_object_init(void);
static void __attribute__((destructor)) j_object_fini(void);

/**
 * Initializes the object client.
 */
static void
j_object_init(void)
{
	gchar const* object_backend;
	gchar const* object_component;
	gchar const* object_path;

	if (j_object_backend != NULL && j_object_module != NULL)
	{
		return;
	}

	object_backend = j_configuration_get_backend(j_configuration(), J_BACKEND_TYPE_OBJECT);
	object_component = j_configuration_get_backend_component(j_configuration(), J_BACKEND_TYPE_OBJECT);
	object_path = j_configuration_get_backend_path(j_configuration(), J_BACKEND_TYPE_OBJECT);

	if (j_backend_load_client(object_backend, object_component, J_BACKEND_TYPE_OBJECT, &j_object_module, &j_object_backend))
	{
		if (j_object_backend == NULL || !j_backend_object_init(j_object_backend, object_path))
		{
			g_critical("Could not initialize object backend %s.\n", object_backend);
		}
	}
}

/**
 * Shuts down the object client.
 */
static void
j_object_fini(void)
{
	if (j_object_backend == NULL && j_object_module == NULL)
	{
		return;
	}

	if (j_object_backend != NULL)
	{
		j_backend_object_fini(j_object_backend);
		j_object_backend = NULL;
	}

	if (j_object_module != NULL)
	{
		g_module_close(j_object_module);
		j_object_module = NULL;
	}
}

static void
j_transformation_object_create_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JTransformationObject* object = data;

	j_transformation_object_unref(object);
}

static void
j_transformation_object_delete_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JTransformationObject* object = data;

	j_transformation_object_unref(object);
}

static void
j_transformation_object_status_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JTransformationObjectOperation* operation = data;

	j_transformation_object_unref(operation->status.object);

	g_slice_free(JTransformationObjectOperation, operation);
}

static void
j_transformation_object_read_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JTransformationObjectOperation* operation = data;

	j_transformation_object_unref(operation->read.object);

	g_slice_free(JTransformationObjectOperation, operation);
}

static void
j_transformation_object_write_free(gpointer data)
{
	J_TRACE_FUNCTION(NULL);

	JTransformationObjectOperation* operation = data;

	j_transformation_cleanup(operation->write.object->transformation,
				 operation->write.data, operation->write.length, operation->write.offset,
				 J_TRANSFORMATION_CALLER_CLIENT_WRITE);

	j_transformation_object_unref(operation->write.object);

	g_slice_free(JTransformationObjectOperation, operation);
}

static gboolean
j_transformation_object_create_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	// FIXME check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autoptr(JMessage) message = NULL;
	gchar const* namespace;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JTransformationObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

	if (object_backend == NULL)
	{
		/**
		 * Force safe semantics to make the server send a reply.
		 * Otherwise, nasty races can occur when using unsafe semantics:
		 * - The client creates the object and sends its first write.
		 * - The client sends another operation using another connection from the pool.
		 * - The second operation is executed first and fails because the object does not exist.
		 * This does not completely eliminate all races but fixes the common case of create, write, write, ...
		 **/
		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_CREATE, namespace_len);
		j_message_set_semantics(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JTransformationObject* object = j_list_iterator_get(it);
		JTransformationObjectMetadata* mdata = NULL;
		g_autoptr(JBatch) kv_batch = NULL;

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

		kv_batch = j_batch_new(semantics);

		mdata = g_new(JTransformationObjectMetadata, 1);
		mdata->transformation_type = object->transformation->type;
		mdata->transformation_mode = object->transformation->mode;
		mdata->original_size = object->original_size;
		mdata->transformed_size = object->transformed_size;

		j_kv_put(object->metadata, mdata, sizeof(JTransformationObjectMetadata), g_free, kv_batch);
		ret = j_batch_execute(kv_batch);
	}

	if (object_backend == NULL)
	{
		JSemanticsSafety safety;

		gpointer object_connection;

		safety = j_semantics_get(semantics, J_SEMANTICS_SAFETY);
		object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, index);
		j_message_send(message, object_connection);

		if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
		{
			g_autoptr(JMessage) reply = NULL;

			reply = j_message_new_reply(message);
			j_message_receive(reply, object_connection);

			/* FIXME do something with reply */
		}

		j_connection_pool_push(J_BACKEND_TYPE_OBJECT, index, object_connection);
	}

	return ret;
}

static gboolean
j_transformation_object_delete_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	// FIXME check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	g_autoptr(JListIterator) it = NULL;
	g_autoptr(JMessage) message = NULL;
	gchar const* namespace;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JTransformationObject* object;

		object = j_list_get_first(operations);
		g_assert(object != NULL);

		namespace = object->namespace;
		namespace_len = strlen(namespace) + 1;
		index = object->index;
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

	if (object_backend == NULL)
	{
		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_DELETE, namespace_len);
		j_message_set_semantics(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JTransformationObject* object = j_list_iterator_get(it);

		// Delete the metadata entry in the kv-store
		g_autoptr(JBatch) kv_batch = NULL;
		kv_batch = j_batch_new(semantics);
		j_kv_delete(object->metadata, kv_batch);
		ret = j_batch_execute(kv_batch);

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
		JSemanticsSafety safety;

		gpointer object_connection;

		safety = j_semantics_get(semantics, J_SEMANTICS_SAFETY);
		object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, index);
		j_message_send(message, object_connection);

		if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
		{
			g_autoptr(JMessage) reply = NULL;

			reply = j_message_new_reply(message);
			j_message_receive(reply, object_connection);

			/* FIXME do something with reply */
		}

		j_connection_pool_push(J_BACKEND_TYPE_OBJECT, index, object_connection);
	}

	return ret;
}

static void
j_transformation_object_set_transformation(JTransformationObject* object, JTransformationType type, JTransformationMode mode)
{
	object->transformation = j_transformation_new(type, mode);
}

static bool
j_transformation_object_load_transformation(JTransformationObject* object)
{
	bool ret = false;

	g_autoptr(JKVIterator) kv_iter = NULL;
	kv_iter = j_kv_iterator_new(object->namespace, object->name);
	while (j_kv_iterator_next(kv_iter))
	{
		gchar const* key;
		gconstpointer value;
		guint32 len;

		key = j_kv_iterator_get(kv_iter, &value, &len);

		if (g_strcmp0(key, object->name) == 0)
		{
			JTransformationObjectMetadata const* mdata = (JTransformationObjectMetadata const*)value;
			j_transformation_object_set_transformation(object, mdata->transformation_type,
								   mdata->transformation_mode);
			object->original_size = mdata->original_size;
			object->transformed_size = mdata->transformed_size;
			ret = true;
		}
	}
	return ret;
}

static bool
j_transformation_object_load_object_size(JTransformationObject* object)
{
	bool ret = false;

	g_autoptr(JKVIterator) kv_iter = NULL;
	kv_iter = j_kv_iterator_new(object->namespace, object->name);
	while (j_kv_iterator_next(kv_iter))
	{
		gchar const* key;
		gconstpointer value;
		guint32 len;

		key = j_kv_iterator_get(kv_iter, &value, &len);

		if (g_strcmp0(key, object->name) == 0)
		{
			JTransformationObjectMetadata const* mdata = (JTransformationObjectMetadata const*)value;
			object->original_size = mdata->original_size;
			object->transformed_size = mdata->transformed_size;
			ret = true;
		}
	}
	return ret;
}

static gboolean
j_transformation_object_update_stored_metadata(JTransformationObject* object, JSemantics* semantics)
{
	gboolean ret = FALSE;
	g_autoptr(JBatch) kv_batch = NULL;
	JTransformationObjectMetadata* mdata = NULL;

	kv_batch = j_batch_new(semantics);
	mdata = g_new(JTransformationObjectMetadata, 1);

	mdata->transformation_type = object->transformation->type;
	mdata->transformation_mode = object->transformation->mode;
	mdata->original_size = object->original_size;
	mdata->transformed_size = object->transformed_size;

	j_kv_put(object->metadata, mdata, sizeof(JTransformationObjectMetadata), g_free, kv_batch);
	ret = j_batch_execute(kv_batch);

	return ret;
}

static gboolean
j_transformation_object_read_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	// FIXME check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	JListIterator* it;
	g_autoptr(JMessage) message = NULL;
	JTransformationObject* object;
	JTransformation* transformation;
	gpointer object_handle;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JTransformationObjectOperation* operation = j_list_get_first(operations);

		object = operation->read.object;

		g_assert(operation != NULL);
		g_assert(object != NULL);

		transformation = object->transformation;

		if (transformation == NULL)
		{
			j_transformation_object_load_transformation(object);
			transformation = object->transformation;
			g_assert(transformation != NULL);
		}
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

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

		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_READ, namespace_len + name_len);
		j_message_set_semantics(message, semantics);
		j_message_append_n(message, object->namespace, namespace_len);
		j_message_append_n(message, object->name, name_len);
	}
	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("object", path);
	}
	*/

	// Find out if the whole object has to be read to perform the retransformation of the read data
	if (transformation->mode == J_TRANSFORMATION_MODE_CLIENT && j_transformation_need_whole_object(transformation, J_TRANSFORMATION_CALLER_CLIENT_READ))
	{
		while (j_list_iterator_next(it))
		{
			JTransformationObjectOperation* operation;
			gpointer transformed_data;
			guint64 transformed_length;
			guint64 offset;
			guint64* bytes_read;

			j_transformation_object_load_object_size(object);

			operation = j_list_iterator_get(it);
			transformed_length = object->transformed_size;
			offset = 0;
			bytes_read = operation->read.bytes_read;

			j_trace_file_begin(object->name, J_TRACE_FILE_READ);

			if (object_backend != NULL)
			{
				guint64 nbytes = 0;
				gpointer whole_data_buf = NULL;
				guint64 data_size = object->transformed_size;
                transformed_data = malloc(object->transformed_size);

				ret = j_backend_object_read(object_backend, object_handle, transformed_data,
							    transformed_length, offset, &nbytes)
				      && ret;
                

                //TODO wip
				j_transformation_apply(transformation, transformed_data, transformed_length,
						       offset, &whole_data_buf, &data_size, &offset,
						       J_TRANSFORMATION_CALLER_CLIENT_READ);
				memcpy(operation->read.data, ((char*)whole_data_buf) + operation->read.offset,
				       operation->read.length);

                // Add the number of read bytes that will be returned to the user
                j_helper_atomic_add(bytes_read, operation->read.length);

				free(transformed_data);
				j_transformation_cleanup(transformation, whole_data_buf, data_size, offset,
							 J_TRANSFORMATION_CALLER_CLIENT_READ);
			}
			else
			{
				j_message_add_operation(message, sizeof(guint64) + sizeof(guint64)
									 + sizeof(JTransformation) + sizeof(guint64) + sizeof(guint64));
				j_message_append_8(message, &transformed_length);
				j_message_append_8(message, &offset);
				j_message_append_n(message, transformation, sizeof(JTransformation));
				j_message_append_8(message, &object->original_size);
				j_message_append_8(message, &object->transformed_size);
			}

			j_trace_file_end(object->name, J_TRACE_FILE_READ, transformed_length, offset);
		}

		j_list_iterator_free(it);

		if (object_backend != NULL)
		{
			ret = j_backend_object_close(object_backend, object_handle) && ret;
		}
		else
		{
			g_autoptr(JMessage) reply = NULL;
			gpointer object_connection;
			guint32 operations_done;
			guint32 operation_count;

			object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, object->index);
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
					JTransformationObjectOperation* operation;
					guint64 transformed_length;
					guint64 offset;
					guint64 nbytes;
					guint64* bytes_read;
					gpointer transformed_data;

					j_transformation_object_load_object_size(object);

					operation = j_list_iterator_get(it);
					transformed_length = object->transformed_size;
					offset = 0;
					bytes_read = operation->read.bytes_read;
					transformed_data = malloc(object->transformed_size);

					nbytes = j_message_get_8(reply);

					if (nbytes > 0)
					{
						GInputStream* input;
						gpointer whole_data_buf = NULL;
						guint64 data_size = object->original_size;

						input = g_io_stream_get_input_stream(G_IO_STREAM(object_connection));
						g_input_stream_read_all(input, transformed_data, nbytes, NULL, NULL, NULL);

						j_transformation_apply(transformation, transformed_data,
                                transformed_length, offset, &whole_data_buf, &data_size,
                                &offset, J_TRANSFORMATION_CALLER_CLIENT_READ);
						memcpy(operation->read.data, ((char*)whole_data_buf) + operation->read.offset, operation->read.length);

                        // Add the number of read bytes that will be returned to the user
                        j_helper_atomic_add(bytes_read, operation->read.length);

						free(transformed_data);
						j_transformation_cleanup(transformation, whole_data_buf, data_size, offset,
									 J_TRANSFORMATION_CALLER_CLIENT_READ);
					}
				}

				operations_done += reply_operation_count;
			}

			j_list_iterator_free(it);

			j_connection_pool_push(J_BACKEND_TYPE_OBJECT, object->index, object_connection);
		}
	}
	// In place modification of the object data are possible and the only thing that needs to
	// be done is transforming the data of the read
	else if (transformation->mode == J_TRANSFORMATION_MODE_CLIENT && !j_transformation_need_whole_object(transformation, J_TRANSFORMATION_CALLER_CLIENT_READ))
	{
		while (j_list_iterator_next(it))
		{
			JTransformationObjectOperation* operation = j_list_iterator_get(it);
			gpointer data = operation->read.data;
			guint64 length = operation->read.length;
			guint64 offset = operation->read.offset;
			guint64* bytes_read = operation->read.bytes_read;

			j_trace_file_begin(object->name, J_TRACE_FILE_READ);

			if (object_backend != NULL)
			{
				guint64 nbytes = 0;

				ret = j_backend_object_read(object_backend, object_handle, data, length, offset, &nbytes) && ret;
				j_helper_atomic_add(bytes_read, nbytes);

				j_transformation_apply(transformation, data, length, offset, &data, &length, &offset,
						       J_TRANSFORMATION_CALLER_CLIENT_READ);
			}
			else
			{
				j_message_add_operation(message, sizeof(guint64) + sizeof(guint64)
									 + sizeof(JTransformation) + sizeof(guint64) + sizeof(guint64));
				j_message_append_8(message, &length);
				j_message_append_8(message, &offset);
				j_message_append_n(message, transformation, sizeof(JTransformation));
				j_message_append_8(message, &object->original_size);
				j_message_append_8(message, &object->transformed_size);
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
			gpointer object_connection;
			guint32 operations_done;
			guint32 operation_count;

			object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, object->index);
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

						j_transformation_apply(transformation, data, length, offset, &data, &length, &offset,
								       J_TRANSFORMATION_CALLER_CLIENT_READ);
					}
				}

				operations_done += reply_operation_count;
			}

			j_list_iterator_free(it);

			j_connection_pool_push(J_BACKEND_TYPE_OBJECT, object->index, object_connection);
		}
	}
	else if (transformation->mode == J_TRANSFORMATION_MODE_SERVER)
	{
		while (j_list_iterator_next(it))
		{
			JTransformationObjectOperation* operation = j_list_iterator_get(it);
			gpointer data = operation->read.data;
			guint64 length = operation->read.length;
			guint64 offset = operation->read.offset;
			guint64* bytes_read = operation->read.bytes_read;

			j_trace_file_begin(object->name, J_TRACE_FILE_READ);

			if (object_backend != NULL)
			{
				guint64 nbytes = 0;

				ret = j_backend_transformation_object_read(object_backend, object_handle, data, length, offset, &nbytes, transformation, &object->original_size, &object->transformed_size) && ret;
				j_helper_atomic_add(bytes_read, nbytes);
			}
			else
			{
				j_message_add_operation(message, sizeof(guint64) + sizeof(guint64)
									 + sizeof(JTransformation) + sizeof(guint64) + sizeof(guint64));
				j_message_append_8(message, &length);
				j_message_append_8(message, &offset);
				j_message_append_n(message, transformation, sizeof(JTransformation));
				j_message_append_8(message, &object->original_size);
				j_message_append_8(message, &object->transformed_size);
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
			gpointer object_connection;
			guint32 operations_done;
			guint32 operation_count;

			object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, object->index);
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
				}

				operations_done += reply_operation_count;
			}

			j_list_iterator_free(it);

			j_connection_pool_push(J_BACKEND_TYPE_OBJECT, object->index, object_connection);
		}
	}

	/*
	if (lock != NULL)
	{
		// FIXME busy wait
		while (!j_lock_acquire(lock));

		j_lock_free(lock);
	}
	*/

	return ret;
}

static gboolean
j_transformation_object_write_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	// FIXME check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	JListIterator* it;
	g_autoptr(JMessage) message = NULL;
	JTransformationObject* object;
	JTransformation* transformation;
	gpointer object_handle;

	// FIXME
	//JLock* lock = NULL;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	{
		JTransformationObjectOperation* operation = j_list_get_first(operations);

		object = operation->write.object;
		transformation = object->transformation;

		g_assert(operation != NULL);
		g_assert(object != NULL);

		if (transformation == NULL)
		{
			j_transformation_object_load_transformation(object);
			transformation = object->transformation;
			g_assert(transformation != NULL);
		}
	}

	it = j_list_iterator_new(operations);
	object_backend = j_object_get_backend();

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

		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_WRITE, namespace_len + name_len);
		j_message_set_semantics(message, semantics);
		j_message_append_n(message, object->namespace, namespace_len);
		j_message_append_n(message, object->name, name_len);
	}

	/*
	if (j_semantics_get(semantics, J_SEMANTICS_ATOMICITY) != J_SEMANTICS_ATOMICITY_NONE)
	{
		lock = j_lock_new("object", path);
	}
	*/

	// Find out if the whole object has to be read to perform the write to the transformed data
	if (transformation->mode == J_TRANSFORMATION_MODE_CLIENT && j_transformation_need_whole_object(transformation, J_TRANSFORMATION_CALLER_CLIENT_WRITE))
	{
		while (j_list_iterator_next(it))
		{
			JTransformationObjectOperation* operation = j_list_iterator_get(it);
			gconstpointer write_data = operation->write.data;
			guint64 write_length = operation->write.length;
			guint64 write_offset = operation->write.offset;
			guint64* bytes_written = operation->write.bytes_written;
			guint64 data_size;
			guint64 off;
			guint64 new_size = 0;
			gpointer whole_data_buf = NULL;
			gpointer transformed_data = NULL;

			j_transformation_object_load_object_size(object);

			//If the object is not empty we need to read all of the transformed data
			if (object->original_size != 0)
			{
				g_autoptr(JBatch) read_batch = NULL;
				guint64 bytes_read = 0;

				if (object->original_size < write_offset + write_length)
				{
					new_size = write_offset + write_length;
				}
				else
				{
					new_size = object->original_size;
				}

				whole_data_buf = malloc(new_size);
				read_batch = j_batch_new(semantics);

				j_transformation_object_read(object, whole_data_buf, object->original_size, 0,
							     &bytes_read, read_batch);
				ret = j_batch_execute(read_batch);
			}
			else
			{
				new_size = write_length;
				whole_data_buf = malloc(new_size);
			}

			object->original_size = new_size;

			j_trace_file_begin(object->name, J_TRACE_FILE_WRITE);

			/*
            if (lock != NULL)
            {
                j_lock_add(lock, block_id);
            }
            */

			// Do the write to the untransformed data
			memcpy(((gchar*)whole_data_buf) + write_offset, write_data, write_length);

			transformed_data = NULL;
			data_size = object->original_size;
			off = 0;

			// Modify the data buffer by applying the transformation
			j_transformation_apply(transformation, whole_data_buf, data_size, off,
					       &transformed_data, &data_size, &off, J_TRANSFORMATION_CALLER_CLIENT_WRITE);

			free(whole_data_buf);
            
			// Store a pointer to the newly created buffer from jtransformation_apply in the operation
			// so that it can be freed in _write_free
			operation->write.data = transformed_data;
			object->transformed_size = data_size;
			j_transformation_object_update_stored_metadata(object, semantics);

			if (object_backend != NULL)
			{
				guint64 nbytes = 0;

				ret = j_backend_object_write(object_backend, object_handle, transformed_data, data_size,
							     off, &nbytes)
				      && ret;
				j_helper_atomic_add(bytes_written, nbytes);
                g_slice_free1(data_size, transformed_data);
			}
			else
			{
				j_message_add_operation(message, sizeof(guint64) + sizeof(guint64)
									 + sizeof(JTransformation) + sizeof(guint64) + sizeof(guint64));
				j_message_append_8(message, &data_size);
				j_message_append_8(message, &off);
				j_message_append_n(message, transformation, sizeof(JTransformation));
				j_message_append_8(message, &object->original_size);
				j_message_append_8(message, &object->transformed_size);
				j_message_add_send(message, transformed_data, data_size);

				// Fake bytes_written here instead of doing another loop further down
				if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) == J_SEMANTICS_SAFETY_NONE)
				{
					j_helper_atomic_add(bytes_written, data_size);
				}
			}

			j_trace_file_end(object->name, J_TRACE_FILE_WRITE, write_length, write_offset);
		}

		j_list_iterator_free(it);

		if (object_backend != NULL)
		{
			ret = j_backend_object_close(object_backend, object_handle) && ret;
		}
		else
		{
			JSemanticsSafety safety;

			gpointer object_connection;

			safety = j_semantics_get(semantics, J_SEMANTICS_SAFETY);
			object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, object->index);
			j_message_send(message, object_connection);

			if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
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

			j_connection_pool_push(J_BACKEND_TYPE_OBJECT, object->index, object_connection);
		}

	}
	// In place modification of the object data are possible and the only thing that needs to
	// be done is transforming the data of the write
	else if (transformation->mode == J_TRANSFORMATION_MODE_CLIENT && !j_transformation_need_whole_object(transformation, J_TRANSFORMATION_CALLER_CLIENT_WRITE))
	{
		while (j_list_iterator_next(it))
		{
			JTransformationObjectOperation* operation = j_list_iterator_get(it);
			gpointer data = operation->write.data;
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

			// Modify the data buffer by applying the transformation
			j_transformation_apply(transformation, data, length, offset, &data, &length,
					       &offset, J_TRANSFORMATION_CALLER_CLIENT_WRITE);
			// Store a pointer to the newly created buffer from jtransformation_apply in the operation
			// so that it can be freed in _write_free
			operation->write.data = data;

			if (object_backend != NULL)
			{
				guint64 nbytes = 0;

				ret = j_backend_object_write(object_backend, object_handle, data, length, offset, &nbytes) && ret;
				j_helper_atomic_add(bytes_written, nbytes);
			}
			else
			{
				j_message_add_operation(message, sizeof(guint64) + sizeof(guint64)
									 + sizeof(JTransformation) + sizeof(guint64) + sizeof(guint64));
				j_message_append_8(message, &length);
				j_message_append_8(message, &offset);
				j_message_append_n(message, transformation, sizeof(JTransformation));
				j_message_append_8(message, &object->original_size);
				j_message_append_8(message, &object->transformed_size);
				j_message_add_send(message, data, length);

				// Fake bytes_written here instead of doing another loop further down
				if (j_semantics_get(semantics, J_SEMANTICS_SAFETY) == J_SEMANTICS_SAFETY_NONE)
				{
					j_helper_atomic_add(bytes_written, length);
				}
			}

			// If neccessary update original_size and transformed_size
			j_transformation_object_load_object_size(object);
			if (offset + length > object->original_size)
			{
				object->original_size = offset + length;
				object->transformed_size = offset + length;
				j_transformation_object_update_stored_metadata(object, semantics);
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
			JSemanticsSafety safety;

			gpointer object_connection;

			safety = j_semantics_get(semantics, J_SEMANTICS_SAFETY);
			object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, object->index);
			j_message_send(message, object_connection);

			if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
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

			j_connection_pool_push(J_BACKEND_TYPE_OBJECT, object->index, object_connection);
		}
	}
	else if (transformation->mode == J_TRANSFORMATION_MODE_SERVER)
	{
		while (j_list_iterator_next(it))
		{
			JTransformationObjectOperation* operation = j_list_iterator_get(it);
			gpointer data = operation->write.data;
			guint64 length = operation->write.length;
			guint64 offset = operation->write.offset;
			guint64* bytes_written = operation->write.bytes_written;

			j_transformation_object_load_object_size(object);

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

				ret = j_backend_transformation_object_write(object_backend, object_handle, data, length, offset, &nbytes, transformation, &object->original_size, &object->transformed_size) && ret;
				j_helper_atomic_add(bytes_written, nbytes);

				j_transformation_object_update_stored_metadata(object, semantics);
			}
			else
			{
				j_message_add_operation(message, sizeof(guint64) + sizeof(guint64)
									 + sizeof(JTransformation) + sizeof(guint64) + sizeof(guint64));
				j_message_append_8(message, &length);
				j_message_append_8(message, &offset);
				j_message_append_n(message, transformation, sizeof(JTransformation));
				j_message_append_8(message, &object->original_size);
				j_message_append_8(message, &object->transformed_size);
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
			JSemanticsSafety safety;

			gpointer object_connection;

			safety = j_semantics_get(semantics, J_SEMANTICS_SAFETY);
			object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, object->index);
			j_message_send(message, object_connection);

			if (safety == J_SEMANTICS_SAFETY_NETWORK || safety == J_SEMANTICS_SAFETY_STORAGE)
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

					object->original_size = j_message_get_8(reply);
					object->transformed_size = j_message_get_8(reply);
					j_transformation_object_update_stored_metadata(object, semantics);
				}

				j_list_iterator_free(it);
			}

			j_connection_pool_push(J_BACKEND_TYPE_OBJECT, object->index, object_connection);
		}
	}

	/*
	if (lock != NULL)
	{
		// FIXME busy wait
		while (!j_lock_acquire(lock));

		j_lock_free(lock);
	}
	*/

	return ret;
}

static gboolean
j_transformation_object_status_exec(JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);

	// FIXME check return value for messages
	gboolean ret = TRUE;

	JBackend* object_backend;
	JListIterator* it;
	g_autoptr(JMessage) message = NULL;
	gchar const* namespace;
	gsize namespace_len;
	guint32 index;

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

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
	object_backend = j_object_get_backend();

	if (object_backend == NULL)
	{
		message = j_message_new(J_MESSAGE_TRANSFORMATION_OBJECT_STATUS, namespace_len);
		j_message_set_semantics(message, semantics);
		j_message_append_n(message, namespace, namespace_len);
	}

	while (j_list_iterator_next(it))
	{
		JTransformationObjectOperation* operation = j_list_iterator_get(it);
		JTransformationObject* object = operation->status.object;
		gint64* modification_time = operation->status.modification_time;
		guint64* size = operation->status.original_size;

		if (object_backend != NULL)
		{
			gpointer object_handle;

			ret = j_backend_object_open(object_backend, object->namespace, object->name, &object_handle) && ret;
			ret = j_backend_object_status(object_backend, object_handle, modification_time, size) && ret;
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

	j_list_iterator_free(it);

	if (object_backend == NULL)
	{
		g_autoptr(JMessage) reply = NULL;
		gpointer object_connection;

		object_connection = j_connection_pool_pop(J_BACKEND_TYPE_OBJECT, index);
		j_message_send(message, object_connection);

		reply = j_message_new_reply(message);
		j_message_receive(reply, object_connection);

		it = j_list_iterator_new(operations);

		while (j_list_iterator_next(it))
		{
			JTransformationObjectOperation* operation = j_list_iterator_get(it);
			gint64* modification_time = operation->status.modification_time;
			guint64* size = operation->status.original_size;
			guint64* transformed_size = operation->status.transformed_size;
			JTransformationType* transformation_type = operation->status.transformation_type;
			gint64 modification_time_;

			modification_time_ = j_message_get_8(reply);

			// Update the object from the kv-store metadata
			j_transformation_object_load_transformation(operation->status.object);

			if (modification_time != NULL)
			{
				*modification_time = modification_time_;
			}

			if (size != NULL)
			{
				*size = operation->status.object->original_size;
			}

			if (transformed_size != NULL)
			{
				*transformed_size = operation->status.object->transformed_size;
			}

			if (transformation_type != NULL)
			{
				*transformation_type = operation->status.object->transformation->type;
			}
		}

		j_list_iterator_free(it);

		j_connection_pool_push(J_BACKEND_TYPE_OBJECT, index, object_connection);
	}

	return ret;
}

/**
 * Creates a new TransformationObject
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * JTransformationObject* i;
 *
 * i = j_transformation_object_new("JULEA");
 * \endcode
 *
 * \param namespace The objects namespace
 * \param name The objects name
 *
 * \return A new object. Should be freed with j_transformation_object_unref().
 **/
JTransformationObject*
j_transformation_object_new(gchar const* namespace, gchar const* name)
{
	J_TRACE_FUNCTION(NULL);

	JConfiguration* configuration = j_configuration();
	JTransformationObject* object;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);

	object = g_slice_new(JTransformationObject);
	object->index = j_helper_hash(name) % j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT);
	object->namespace = g_strdup(namespace);
	object->name = g_strdup(name);
	object->ref_count = 1;

	object->metadata = j_kv_new(namespace, name);
	object->original_size = 0;
	object->transformed_size = 0;
	object->transformation = NULL;

	return object;
}

/**
 * Creates a new TransformationObject.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param index The wanted index
 * \param namespace The objects namespace
 * \param name The objects name
 *
 * \return A new object. Should be freed with j_transformation_object_unref().
 **/
JTransformationObject*
j_transformation_object_new_for_index(guint32 index, gchar const* namespace, gchar const* name)
{
	J_TRACE_FUNCTION(NULL);

	JConfiguration* configuration = j_configuration();
	JTransformationObject* object;

	g_return_val_if_fail(namespace != NULL, NULL);
	g_return_val_if_fail(name != NULL, NULL);
	g_return_val_if_fail(index < j_configuration_get_server_count(configuration, J_BACKEND_TYPE_OBJECT), NULL);

	object = g_slice_new(JTransformationObject);
	object->index = index;
	object->namespace = g_strdup(namespace);
	object->name = g_strdup(name);
	object->ref_count = 1;

	object->metadata = j_kv_new(namespace, name);
	object->original_size = 0;
	object->transformed_size = 0;

	return object;
}

/**
 * Increases an object's reference count.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * JTransformationObject* i;
 *
 * j_transformation_object_ref(i);
 * \endcode
 *
 * \param object An object
 *
 * \return #object
 **/
JTransformationObject*
j_transformation_object_ref(JTransformationObject* object)
{
	J_TRACE_FUNCTION(NULL);

	g_return_val_if_fail(object != NULL, NULL);

	g_atomic_int_inc(&(object->ref_count));

	return object;
}

/**
 * Decreases an object's reference count.
 * When the reference count reaches zero, frees the memory allocated for the object.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param object An object.
 **/
void
j_transformation_object_unref(JTransformationObject* object)
{
	J_TRACE_FUNCTION(NULL);

	g_return_if_fail(object != NULL);

	if (g_atomic_int_dec_and_test(&(object->ref_count)))
	{
		g_free(object->name);
		g_free(object->namespace);

		g_slice_free(JTransformationObject, object);
	}
}

/**
 * Creates an object.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param object A pointer to the created object
 * \param batch A batch
 * \param type The transformation type
 * \param mode The transformation mode
 *
 * \return A new object. Should be freed with j_transformation_object_unref().
 **/
void
j_transformation_object_create(JTransformationObject* object, JBatch* batch, JTransformationType type, JTransformationMode mode)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* operation;

	g_return_if_fail(object != NULL);

	object->original_size = 0;
	object->transformed_size = 0;
	j_transformation_object_set_transformation(object, type, mode);

	operation = j_operation_new();
	// FIXME key = index + namespace
	operation->key = object;
	operation->data = j_transformation_object_ref(object);
	operation->exec_func = j_transformation_object_create_exec;
	operation->free_func = j_transformation_object_create_free;

	j_batch_add(batch, operation);
}

/**
 * Deletes an object.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param object       An object.
 * \param batch      A batch.
 **/
void
j_transformation_object_delete(JTransformationObject* object, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JOperation* operation;

	g_return_if_fail(object != NULL);

	operation = j_operation_new();
	operation->key = object;
	operation->data = j_transformation_object_ref(object);
	operation->exec_func = j_transformation_object_delete_exec;
	operation->free_func = j_transformation_object_delete_free;

	j_batch_add(batch, operation);
}

/**
 * Reads an object.
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
j_transformation_object_read(JTransformationObject* object, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JTransformationObjectOperation* iop;
	JOperation* operation;
	guint64 max_operation_size;

	g_return_if_fail(object != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);
	g_return_if_fail(bytes_read != NULL);

	max_operation_size = j_configuration_get_max_operation_size(j_configuration());

	// Chunk operation if necessary
	while (length > 0)
	{
		guint64 chunk_size;

		chunk_size = MIN(length, max_operation_size);

		iop = g_slice_new(JTransformationObjectOperation);
		iop->read.object = j_transformation_object_ref(object);
		iop->read.data = data;
		iop->read.length = chunk_size;
		iop->read.offset = offset;
		iop->read.bytes_read = bytes_read;

		operation = j_operation_new();
		operation->key = object;
		operation->data = iop;
		operation->exec_func = j_transformation_object_read_exec;
		operation->free_func = j_transformation_object_read_free;

		j_batch_add(batch, operation);

		data = (gchar*)data + chunk_size;
		length -= chunk_size;
		offset += chunk_size;
	}

	*bytes_read = 0;
}

/**
 * Writes an object.
 *
 * \note
 * j_transformation_object_write() modifies bytes_written even if j_batch_execute() is not called.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param object          An object.
 * \param data          A buffer holding the data to write.
 * \param length        Number of bytes to write.
 * \param offset        An offset within #object.
 * \param bytes_written Number of bytes written.
 * \param batch         A batch.
 **/
void
j_transformation_object_write(JTransformationObject* object, gpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JTransformationObjectOperation* iop;
	JOperation* operation;
	guint64 max_operation_size;

	g_return_if_fail(object != NULL);
	g_return_if_fail(data != NULL);
	g_return_if_fail(length > 0);
	g_return_if_fail(bytes_written != NULL);

	max_operation_size = j_configuration_get_max_operation_size(j_configuration());

	// Chunk operation if necessary
	while (length > 0)
	{
		guint64 chunk_size;

		chunk_size = MIN(length, max_operation_size);

		iop = g_slice_new(JTransformationObjectOperation);
		iop->write.object = j_transformation_object_ref(object);
		iop->write.data = data;
		iop->write.length = chunk_size;
		iop->write.offset = offset;
		iop->write.bytes_written = bytes_written;

		operation = j_operation_new();
		operation->key = object;
		operation->data = iop;
		operation->exec_func = j_transformation_object_write_exec;
		operation->free_func = j_transformation_object_write_free;

		j_batch_add(batch, operation);

		data = (gchar*)data + chunk_size;
		length -= chunk_size;
		offset += chunk_size;
	}

	*bytes_written = 0;
}

/**
 * Get the status of an object.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param object A pointer to an object
 * \param modification_time Return parameter for the last modification time of the object
 * \param size Return parameter for the objects original size
 * \param batch A batch
 **/
void
j_transformation_object_status(JTransformationObject* object, gint64* modification_time,
			       guint64* size, JBatch* batch)
{
	j_transformation_object_status_ext(object, modification_time, size, NULL, NULL, batch);
}

/**
 * Get the status of an object with transformation properties.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * \endcode
 *
 * \param object A pointer to the object
 * \param modification_time Return parameter for the last modification timestamp
 * \param original_size Return parameter for the original size of the objects data
 * \param transformed_size Return parameter for the transformed size of the objects data
 * \param transformation type Return parameter the objects transformation type
 * \param batch A batch
 **/
void
j_transformation_object_status_ext(JTransformationObject* object, gint64* modification_time,
				   guint64* original_size, guint64* transformed_size, JTransformationType* transformation_type, JBatch* batch)
{
	J_TRACE_FUNCTION(NULL);

	JTransformationObjectOperation* iop;
	JOperation* operation;

	g_return_if_fail(object != NULL);

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
}

/**
 * Returns the object backend.
 *
 * \return The object backend.
 */
JBackend*
j_object_get_backend(void)
{
	return j_object_backend;
}

/**
 * @}
 **/
