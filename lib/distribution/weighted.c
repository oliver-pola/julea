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

#include <glib.h>

#include <bson.h>

#include <jconfiguration.h>
#include <jhelper-internal.h>
#include <jtrace-internal.h>

#include <julea-internal.h>

#include "distribution.h"

/**
 * \defgroup JDistribution Distribution
 *
 * Data structures and functions for managing distributions.
 *
 * @{
 **/

/**
 * A distribution.
 **/
struct JDistributionWeighted
{
	/**
	 * The server count.
	 **/
	guint server_count;

	/**
	 * The length.
	 **/
	guint64 length;

	/**
	 * The offset.
	 **/
	guint64 offset;

	/**
	 * The block size.
	 */
	guint64 block_size;

	guint* weights;
	guint sum;
};

typedef struct JDistributionWeighted JDistributionWeighted;

/**
 * Distributes data to a weighted list of servers.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param index        A server index.
 * \param new_length   A new length.
 * \param new_offset   A new offset.
 *
 * \return TRUE on success, FALSE if the distribution is finished.
 **/
static
gboolean
distribution_distribute (gpointer data, guint* index, guint64* new_length, guint64* new_offset, guint64* block_id)
{
	JDistributionWeighted* distribution = data;

	gboolean ret = TRUE;
	guint64 block;
	guint64 displacement;
	guint64 round;
	guint block_offset;

	j_trace_enter(G_STRFUNC, NULL);

	if (distribution->length == 0)
	{
		ret = FALSE;
		goto end;
	}

	block = distribution->offset / distribution->block_size;
	round = block / distribution->sum;
	displacement = distribution->offset % distribution->block_size;

	*index = 0;

	block_offset = block % distribution->sum;

	for (guint i = 0; i < distribution->server_count; i++)
	{
		if (block_offset < distribution->weights[i])
		{
			*index = i;
			break;
		}

		block_offset -= distribution->weights[i];
	}

	*new_length = MIN(distribution->length, distribution->block_size - displacement);
	*new_offset = (((round * distribution->weights[*index]) + block_offset) * distribution->block_size) + displacement;
	*block_id = block;

	distribution->length -= *new_length;
	distribution->offset += *new_length;

end:
	j_trace_leave(G_STRFUNC);

	return ret;
}

static
gpointer
distribution_new (guint server_count)
{
	JDistributionWeighted* distribution;

	j_trace_enter(G_STRFUNC, NULL);

	distribution = g_slice_new(JDistributionWeighted);
	distribution->server_count = server_count;
	distribution->length = 0;
	distribution->offset = 0;
	distribution->block_size = J_STRIPE_SIZE;

	distribution->sum = 0;
	distribution->weights = g_new(guint, distribution->server_count);

	for (guint i = 0; i < distribution->server_count; i++)
	{
		distribution->weights[i] = 0;
	}

	j_trace_leave(G_STRFUNC);

	return distribution;
}

/**
 * Decreases a distribution's reference count.
 * When the reference count reaches zero, frees the memory allocated for the distribution.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 **/
static
void
distribution_free (gpointer data)
{
	JDistributionWeighted* distribution = data;

	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	g_free(distribution->weights);

	g_slice_free(JDistributionWeighted, distribution);

	j_trace_leave(G_STRFUNC);
}

/**
 * Sets the start index for the round robin distribution.
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution A distribution.
 * \param start_index  An index.
 */
static
void
distribution_set (gpointer data, gchar const* key, guint64 value)
{
	JDistributionWeighted* distribution = data;

	g_return_if_fail(distribution != NULL);

	if (g_strcmp0(key, "block-size") == 0)
	{
		distribution->block_size = value;
	}
}

static
void
distribution_set2 (gpointer data, gchar const* key, guint64 value1, guint64 value2)
{
	JDistributionWeighted* distribution = data;

	g_return_if_fail(distribution != NULL);

	if (g_strcmp0(key, "weight") == 0)
	{
		g_return_if_fail(value1 < distribution->server_count);
		g_return_if_fail(value2 < 256);
		g_return_if_fail(distribution->sum + value2 - distribution->weights[value1] > 0);

		distribution->sum += value2 - distribution->weights[value1];
		distribution->weights[value1] = value2;
	}
}

/**
 * Serializes distribution.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution Credentials.
 *
 * \return A new BSON object. Should be freed with g_slice_free().
 **/
static
void
distribution_serialize (gpointer data, bson_t* b)
{
	JDistributionWeighted* distribution = data;

	bson_t b_array[1];
	gchar numstr[16];

	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	bson_append_int64(b, "block_size", -1, distribution->block_size);

	bson_append_array_begin(b, "weights", -1, b_array);

	for (guint i = 0; i < distribution->server_count; i++)
	{
		// FIXME
		j_helper_get_number_string(numstr, sizeof(numstr), i);
		bson_append_int32(b_array, numstr, -1, distribution->weights[i]);
	}

	bson_append_array_end(b, b_array);

	j_trace_leave(G_STRFUNC);
}

/**
 * Deserializes distribution.
 *
 * \private
 *
 * \author Michael Kuhn
 *
 * \code
 * \endcode
 *
 * \param distribution distribution.
 * \param b           A BSON object.
 **/
static
void
distribution_deserialize (gpointer data, bson_t const* b)
{
	JDistributionWeighted* distribution = data;
	bson_iter_t iterator;

	g_return_if_fail(distribution != NULL);
	g_return_if_fail(b != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	bson_iter_init(&iterator, b);

	while (bson_iter_next(&iterator))
	{
		gchar const* key;

		key = bson_iter_key(&iterator);

		if (g_strcmp0(key, "block_size") == 0)
		{
			distribution->block_size = bson_iter_int64(&iterator);
		}
		else if (g_strcmp0(key, "weights") == 0)
		{
			bson_iter_t siterator;

			bson_iter_recurse(&iterator, &siterator);

			distribution->sum = 0;

			for (guint i = 0; bson_iter_next(&siterator); i++)
			{
				distribution->weights[i] = bson_iter_int32(&siterator);
				distribution->sum += distribution->weights[i];
			}
		}
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * Initializes a distribution.
 *
 * \author Michael Kuhn
 *
 * \code
 * JDistribution* d;
 *
 * j_distribution_init(d, 0, 0);
 * \endcode
 *
 * \param length A length.
 * \param offset An offset.
 *
 * \return A new distribution. Should be freed with j_distribution_unref().
 **/
static
void
distribution_reset (gpointer data, guint64 length, guint64 offset)
{
	JDistributionWeighted* distribution = data;

	g_return_if_fail(distribution != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	distribution->length = length;
	distribution->offset = offset;

	j_trace_leave(G_STRFUNC);
}

void
j_distribution_weighted_get_vtable (JDistributionVTable* vtable)
{
	vtable->distribution_new = distribution_new;
	vtable->distribution_free = distribution_free;
	vtable->distribution_set = distribution_set;
	vtable->distribution_set2 = distribution_set2;
	vtable->distribution_serialize = distribution_serialize;
	vtable->distribution_deserialize = distribution_deserialize;
	vtable->distribution_reset = distribution_reset;
	vtable->distribution_distribute = distribution_distribute;
}

/**
 * @}
 **/
