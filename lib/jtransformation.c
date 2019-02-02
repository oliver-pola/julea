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

/**
 * Get a JTransformation object from type (and params)
 **/
JTransformation* j_transformation_new (JTransformationType type, void* params)
{
    JTransformation* trafo;

    (void)params; // unused

    trafo = g_slice_new(JTransformation);

    trafo->type = type;

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
 * @}
 **/
