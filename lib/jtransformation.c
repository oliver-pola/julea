/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2018 Michael Kuhn
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

#include <jtransformation.h>

#include <glib.h>

/**
 * \defgroup JTransformation Transformation
 * @{
 **/

/**
 * A Transformation
 **/
struct JTransformation
{
    /**
	 * Which transformation to apply.
	 **/
    JTransformationType type;

    /**
	 * Whether client or server applies transformation.
	 **/
    JTransformationMode mode;

    /**
	 * Whether the transformation changes data size.
	 **/
	gboolean changes_size;

    /**
	 * Whether parts of data can be edited without considering the neighbourhood.
	 **/
	gboolean partial_edit;

    /**
	 * The reference count.
	 **/
	gint ref_count;
};

static void j_transformation_apply_xor (gpointer data, guint64 length, guint64 offset)
{
    gint8* d = data + offset;

    for(guint i = 0; i < length; i++)
    {
        d[i] = d[i] ^ 255;
    }
}

static void j_transformation_apply_xor_inverse (gpointer data, guint64 length, guint64 offset)
{
    j_transformation_apply_xor(data, length, offset);
}


/**
 * Get a JTransformation object from type (and params)
 **/
JTransformation* j_transformation_new (JTransformationType type,
    JTransformationMode mode, void* params)
{
    JTransformation* trafo;

    (void)params; // unused

    trafo = g_slice_new(JTransformation);

    trafo->type = type;
    trafo->mode = mode;
    trafo->ref_count = 1;

    switch (type)
    {
        case J_TRANSFORMATION_TYPE_NONE:
            trafo->changes_size = FALSE;
            trafo->partial_edit = TRUE;
            break;
        case J_TRANSFORMATION_TYPE_XOR:
            trafo->changes_size = FALSE;
            trafo->partial_edit = TRUE;
            break;
        default:
            trafo->changes_size = FALSE;
            trafo->partial_edit = TRUE;
    }

    return trafo;
}

JTransformation* j_transformation_ref (JTransformation* item)
{
    g_return_val_if_fail(item != NULL, NULL);

	g_atomic_int_inc(&(item->ref_count));

	return item;
}

void j_transformation_unref (JTransformation* item)
{
    g_return_if_fail(item != NULL);

	if (g_atomic_int_dec_and_test(&(item->ref_count)))
	{
		g_slice_free(JTransformation, item);
	}
}

/**
 * Applies a transformation (inverse) on the data with length and offset.
 * This is done inplace (with an internal copy if necessary).
 **/
void j_transformation_apply (JTransformation* trafo, JTransformationCaller caller,
    gpointer data, guint64 length, guint64 offset)
{
    gboolean inverse = FALSE;

    g_return_if_fail(trafo != NULL);

    // Decide who needs to do transform and who inverse transform
    switch (trafo->mode)
    {
        default:
        case J_TRANSFORMATION_MODE_CLIENT:
            if (caller == J_TRANSFORMATION_CALLER_SERVER_READ ||
                caller == J_TRANSFORMATION_CALLER_SERVER_WRITE)
                return;
            if (caller == J_TRANSFORMATION_CALLER_CLIENT_READ)
                inverse = TRUE;
            break;
        case J_TRANSFORMATION_MODE_TRANSPORT:
            if (caller == J_TRANSFORMATION_CALLER_CLIENT_READ ||
                caller == J_TRANSFORMATION_CALLER_SERVER_WRITE)
                inverse = TRUE;
            break;
        case J_TRANSFORMATION_MODE_SERVER:
            if (caller == J_TRANSFORMATION_CALLER_CLIENT_READ ||
                caller == J_TRANSFORMATION_CALLER_CLIENT_WRITE)
                return;
            if (caller == J_TRANSFORMATION_CALLER_SERVER_READ)
                inverse = TRUE;
            break;
    }
    

    switch (trafo->type)
    {
        case J_TRANSFORMATION_TYPE_NONE:
            break;
        case J_TRANSFORMATION_TYPE_XOR:
            if(inverse)
                j_transformation_apply_xor_inverse(data, length, offset);
            else
                j_transformation_apply_xor(data, length, offset);
            break;
        default:
            break;
    }
}

/**
 * @}
 **/
