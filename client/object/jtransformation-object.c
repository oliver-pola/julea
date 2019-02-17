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
 * Data structures and functions for managing objects.
 *
 * @{
 **/


/**
 * A JObject.
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
 * Creates a new item.
 *
 * \author Michael Kuhn
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
    j_object_set_transform(object->object, transformation_type, transformation_mode, NULL);
    
    j_trace_leave(G_STRFUNC);

    return object;
}

/**
 * Creates a new item.
 *
 * \author Michael Kuhn
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
 * \return A new item. Should be freed with j_object_unref().
 **/
JTransformationObject*
j_transformation_object_new_for_index (guint32 index, gchar const* namespace, gchar const* name, JTransformationType transformation_type, JTransformationMode transformation_mode)
{
    JTransformationObject* object;

	j_trace_enter(G_STRFUNC, NULL);

	object = g_slice_new(JTransformationObject);
    object->object = j_object_new_for_index (index, namespace, name);
	object->ref_count = 1;
    j_object_set_transform(object->object, transformation_type, transformation_mode, NULL);

	j_trace_leave(G_STRFUNC);

	return object;
}

/**
 * Increases an item's reference count.
 *
 * \author Michael Kuhn
 *
 * \code
 * JObject* i;
 *
 * j_object_ref(i);
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
 * \author Michael Kuhn
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
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param name         A name.
 * \param distribution A distribution.
 * \param batch        A batch.
 *
 * \return A new item. Should be freed with j_object_unref().
 **/
void
j_transformation_object_create (JTransformationObject* object, JBatch* batch)
{
    j_object_create(object->object, batch);
}

/**
 * Deletes an object.
 *
 * \author Michael Kuhn
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
 * \author Michael Kuhn
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
    j_object_read(object->object, data, length, offset, bytes_read, batch);
}

/**
 * Writes an item.
 *
 * \note
 * j_object_write() modifies bytes_written even if j_batch_execute() is not called.
 *
 * \author Michael Kuhn
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
    j_object_write(object->object, data, length, offset, bytes_written, batch);
}


/**
 * Get the status of an item.
 *
 * \author Michael Kuhn
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
