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

#include <object/jdistributed-transformation-object.h>
#include <object/jtransformation-object.h>
#include <object/jobject.h>

#include <julea-kv.h>

#include <julea.h>

/**
 * \defgroup JDistributedTransformationObject Object
 *
 * Data structures and functions for managing transformation objects.
 *
 * @{
 **/

struct JDistributedTransformationObjectOperation
{
	union
	{
		struct
		{
			JDistributedTransformationObject* object;
			gint64* modification_time;
			guint64* original_size;
			guint64* transformed_size;
			JTransformationType* transformation_type;
		}
		status;

		struct
		{
			JDistributedTransformationObject* object;
			gpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_read;
		}
		read;

		struct
		{
			JDistributedTransformationObject* object;
			gconstpointer data;
			guint64 length;
			guint64 offset;
			guint64* bytes_written;
		}
		write;
	};
};

typedef struct JDistributedTransformationObjectOperation JDistributedTransformationObjectOperation;

/**
 * A JDistributedTransformationObject.
 **/
struct JDistributedTransformationObject
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
 * TODO
 **/
struct JDistributedTransformationObjectMetadata
{
    gint32 transformation_type;
    gint32 transformation_mode;
    guint64 original_size;
    guint64 transformed_size;
};

typedef struct JDistributedTransformationObjectMetadata JDistributedTransformationObjectMetadata;

static
void
j_distributed_transformation_object_create_free (gpointer data)
{
    //TODO handle child objects?
	J_TRACE_FUNCTION(NULL);

	JDistributedTransformationObject* object = data;

	j_distributed_transformation_object_unref(object);
}

static
void
j_distributed_transformation_object_delete_free (gpointer data)
{
    //TODO handle child objects?
	J_TRACE_FUNCTION(NULL);

	JDistributedTransformationObject* object = data;

	j_distributed_transformation_object_unref(object);
}

static
void
j_distributed_transformation_object_status_free (gpointer data)
{
    //TODO handle child objects?
	J_TRACE_FUNCTION(NULL);

	JDistributedTransformationObjectOperation* operation = data;

	j_distributed_transformation_object_unref(operation->status.object);

	g_slice_free(JDistributedTransformationObjectOperation, operation);
}

static
void
j_distributed_transformation_object_read_free (gpointer data)
{
    //TODO handle child objects?
	J_TRACE_FUNCTION(NULL);

	JDistributedTransformationObjectOperation* operation = data;

	j_distributed_transformation_object_unref(operation->read.object);

	g_slice_free(JDistributedTransformationObjectOperation, operation);
}

static
void
j_distributed_transformation_object_write_free (gpointer data)
{
    //TODO handle child objects?
	J_TRACE_FUNCTION(NULL);

	JDistributedTransformationObjectOperation* operation = data;

    // TODO needed?
    /* j_transformation_cleanup(operation->write.object->transformation,  */
    /*         operation->write.data, operation->write.length, operation->write.offset, */
    /*         J_TRANSFORMATION_CALLER_CLIENT_WRITE); */

	j_distributed_transformation_object_unref(operation->write.object);

	g_slice_free(JDistributedTransformationObjectOperation, operation);
}

static
gboolean
j_distributed_transformation_object_create_exec (JList* operations, JSemantics* semantics)
{
	J_TRACE_FUNCTION(NULL);
	// FIXME check return value for messages
	gboolean ret = TRUE;
}

static
gboolean
j_distributed_transformation_object_delete_exec (JList* operations, JSemantics* semantics)
{
}

static
void
j_distributed_transformation_object_set_transformation(JDistributedTransformationObject* object, JTransformationType type, JTransformationMode mode, void* params)
{
}


static
bool
j_distributed_transformation_object_load_transformation(JDistributedTransformationObject* object, JSemantics* semantics)
{
}

static
bool
j_distributed_transformation_object_load_object_size(JDistributedTransformationObject* object, JSemantics* semantics)
{
}

static
void
j_distributed_transformation_object_update_stored_metadata(JDistributedTransformationObject* object, JSemantics* semantics)
{
}

static
gboolean
j_distributed_transformation_object_read_exec (JList* operations, JSemantics* semantics)
{
}

static
gboolean
j_distributed_transformation_object_write_exec (JList* operations, JSemantics* semantics)
{
}

static
gboolean
j_distributed_transformation_object_status_exec (JList* operations, JSemantics* semantics)
{
}




/**
 * Creates a new item.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * JDistributedTransformationObject* i;
 *
 * i = j_distributed_transformation_object_new("JULEA");
 * \endcode
 *
 * \param name         An item name.
 * \param distribution A distribution.
 *
 * \return A new item. Should be freed with j_distributed_transformation_object_unref().
 **/
JDistributedTransformationObject*
j_distributed_transformation_object_new (gchar const* namespace, gchar const* name)
{
}

/**
 * Creates a new item.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * JDistributedTransformationObject* i;
 *
 * i = j_distributed_transformation_object_new("JULEA");
 * \endcode
 *
 * \param name         An item name.
 * \param distribution A distribution.
 *
 * \return A new item. Should be freed with j_distributed_transformation_object_unref().
 **/
JDistributedTransformationObject*
j_distributed_transformation_object_new_for_index (guint32 index, gchar const* namespace, gchar const* name)
{
}

/**
 * Increases an item's reference count.
 *
 * \author Michael Blesel, Oliver Pola
 *
 * \code
 * JDistributedTransformationObject* i;
 *
 * j_distributed_transformation_object_ref(i);
 * \endcode
 *
 * \param item An item.
 *
 * \return #item.
 **/
JDistributedTransformationObject*
j_distributed_transformation_object_ref (JDistributedTransformationObject* object)
{
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
j_distributed_transformation_object_unref (JDistributedTransformationObject* object)
{
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
 * \return A new item. Should be freed with j_distributed_transformation_object_unref().
 **/
void
j_distributed_transformation_object_create (JDistributedTransformationObject* object, JBatch* batch, JTransformationType type, JTransformationMode mode, void* params)
{
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
j_distributed_transformation_object_delete (JDistributedTransformationObject* object, JBatch* batch)
{
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
j_distributed_transformation_object_read (JDistributedTransformationObject* object, gpointer data, guint64 length, guint64 offset, guint64* bytes_read, JBatch* batch)
{
}

/**
 * Writes an item.
 *
 * \note
 * j_distributed_transformation_object_write() modifies bytes_written even if j_batch_execute() is not called.
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
j_distributed_transformation_object_write (JDistributedTransformationObject* object, gconstpointer data, guint64 length, guint64 offset, guint64* bytes_written, JBatch* batch)
{
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
j_distributed_transformation_object_status (JDistributedTransformationObject* object, gint64* modification_time,
	guint64* size, JBatch* batch)
{
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
j_distributed_transformation_object_status_ext (JDistributedTransformationObject* object, gint64* modification_time,
	guint64* original_size, guint64* transformed_size, JTransformationType* transformation_type, JBatch* batch)
{
}

/**
 * @}
 **/
