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

/**
 * XOR with 1 for each bit
 */
static void j_transformation_apply_xor (gpointer input, gpointer* output,
    guint64* length, guint64* offset)
{
    guint8* in;
    guint8* out;

    in = input;
    in += *offset;

    out = g_slice_alloc(*length);

    for(guint i = 0; i < *length; i++)
    {
        out[i] = in[i] ^ 255;
    }

    *output = out;

    // Buffer is only of size = length, so offset within buffer is now 0
    *offset = 0;
}

static void j_transformation_apply_xor_inverse (gpointer input, gpointer* output,
    guint64* length, guint64* offset)
{
    j_transformation_apply_xor(input, output, length, offset);
}

/**
 * Simple run length encoding
 */
static void j_transformation_apply_rle (gpointer input, gpointer* output,
    guint64* length, guint64* offset)
{
    guint8 value, copies;
    guint8* in = input;
    in += *offset; // probably offset 0 must be enforced

    if (*length > 0)
    {
        copies = 0; // this means count = 1, storing a 0 makes no sense
        value = in[0];
        for (guint i = 1; i < *length; i++)
        {
            if (in[i] == value && copies < 255)
            {
                copies++;
            }
            else
            {
                // TODO: write precount and value to some other buffer
            }
        }
    }
}

static void j_transformation_apply_rle_inverse (gpointer input, gpointer* output,
    guint64* length, guint64* offset)
{
    guint8 value, count;
    // guint8* buffer;
    // giunt8* buffer_pos;
    guint8* in = input;
    in += *offset; // probably offset 0 must be enforced

    // TODO: get buffer
    // buffer_pos = buffer;

    for (guint i = 1; i < *length; i += 2)
    {
        count = in[i - 1] + 1;
        value = in[i];
        // TODO: memset(buffer_pos, value, copies + 1);
        // buffer_pos += count;
    }
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
        case J_TRANSFORMATION_TYPE_RLE:
            trafo->changes_size = TRUE;
            trafo->partial_edit = FALSE;
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
void j_transformation_apply (JTransformation* trafo, gpointer input,
    guint64 inlength, guint64 inoffset, gpointer* output,
    guint64* outlength, guint64* outoffset, JTransformationCaller caller)
{
    // Buffer for output of transformation, needs to be allocated by every method
    // because only there the size is known / estimated
    gpointer buffer;
    guint64 length;
    guint64 offset;
    gboolean inverse;

    length = inlength;
    offset = inoffset;
    inverse = FALSE;

    g_return_if_fail(trafo != NULL);
    g_return_if_fail(input != NULL);
    g_return_if_fail(output != NULL);
    g_return_if_fail(outlength != NULL);
    g_return_if_fail(outoffset != NULL);
    g_return_if_fail(*output != NULL);

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
            return;
        case J_TRANSFORMATION_TYPE_XOR:
            if(inverse)
                j_transformation_apply_xor_inverse(input, &buffer, &length, &offset);
            else
                j_transformation_apply_xor(input, &buffer, &length, &offset);
            break;
        case J_TRANSFORMATION_TYPE_RLE:
            if(inverse)
                j_transformation_apply_rle_inverse(input, &buffer, &length, &offset);
            else
                j_transformation_apply_rle(input, &buffer, &length, &offset);
            break;
        default:
            return;
    }

    // output buffer is always created by the method, but for read we have
    // user app memory as output given, we need to copy the requested part
    // and free the output buffer (cleanup does free the input buffer)
    if ((caller == J_TRANSFORMATION_CALLER_CLIENT_READ) ||
        (caller == J_TRANSFORMATION_CALLER_SERVER_READ))
    {
        g_return_if_fail(buffer != NULL);
        // TODO buffer can now be the whole tranformed object while output
        // only wanted a small part of it
        memcpy(*output, buffer, *outlength);
        g_slice_free1(length, buffer);
    }
    else
    {
        g_return_if_fail(buffer != NULL);
        *output = buffer;
        *outlength = length;
        *outoffset = offset;
    }
}

/**
 * Cleans up after j_transformation_apply is done, frees temp buffer
 *
 * For write operations this needs to be called in write_free() with the data
 * stored in the operation struct, after the data is transfered.
 * For read operations this can be called directly after the transformation was
 * applied and the parameters must be the temp buffer prepared by
 * prep_read_buffer()
 **/
void j_transformation_cleanup (JTransformation* trafo, gpointer data,
    guint64 length, guint64 offset, JTransformationCaller caller)
{
    (void)trafo; // unused
    (void)offset; // unused

    g_return_if_fail(data != NULL);

    // write always needs a temp buffer to not interfer with user app memory
    if (caller == J_TRANSFORMATION_CALLER_CLIENT_WRITE ||
        caller == J_TRANSFORMATION_CALLER_SERVER_WRITE)
    {
        g_slice_free1(length, data);
    }
    // read only needs a buffer if transformation can't be done inplace
    else if (trafo->changes_size || !trafo->partial_edit)
    {
        g_slice_free1(length, data);
    }
}

void j_transformation_prep_read_buffer (JTransformation* trafo, gpointer data,
    guint64 length, guint64 offset, gpointer* buffer, guint64* buflength,
    guint64* bufoffs, JTransformationCaller caller)
{
    // TODO
    (void)trafo; // unused
    (void)data; // unused
    (void)length; // unused
    (void)offset; // unused
    (void)buffer; // unused
    (void)buflength; // unused
    (void)bufoffs; // unused
    (void)caller; // unused
}

/**
 * @}
 **/
