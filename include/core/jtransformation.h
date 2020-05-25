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

#ifndef JULEA_TRANSFORM_H
#define JULEA_TRANSFORM_H

#if !defined(JULEA_H) && !defined(JULEA_COMPILATION)
#error "Only <julea.h> can be included directly."
#endif

#include <glib.h>

G_BEGIN_DECLS

enum JTransformationType
{
	J_TRANSFORMATION_TYPE_NONE,
	J_TRANSFORMATION_TYPE_XOR,
	J_TRANSFORMATION_TYPE_RLE,
	J_TRANSFORMATION_TYPE_LZ4,
};

typedef enum JTransformationType JTransformationType;

enum JTransformationMode
{
	// Client encodes on write, decodes on read
	J_TRANSFORMATION_MODE_CLIENT,

	// Client encodes, server decodes on write
	// Server encodes, client decodes on read
	J_TRANSFORMATION_MODE_TRANSPORT,

	// Server encodes on write, decodes on read
	J_TRANSFORMATION_MODE_SERVER,
};

typedef enum JTransformationMode JTransformationMode;

enum JTransformationCaller
{
	J_TRANSFORMATION_CALLER_CLIENT_READ,
	J_TRANSFORMATION_CALLER_CLIENT_WRITE,
	J_TRANSFORMATION_CALLER_SERVER_READ,
	J_TRANSFORMATION_CALLER_SERVER_WRITE,
};

typedef enum JTransformationCaller JTransformationCaller;

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
	 * Whether parts of data can be read or written without knowing the neighbourhood.
	 **/
	gboolean partial_access;

	/**
	 * The reference count.
	 **/
	gint ref_count;
};
typedef struct JTransformation JTransformation;

JTransformation* j_transformation_new(JTransformationType, JTransformationMode, void*);
JTransformation* j_transformation_ref(JTransformation*);
void j_transformation_unref(JTransformation*);

void j_transformation_apply(JTransformation*, gpointer, guint64, guint64,
			    gpointer*, guint64*, guint64*, JTransformationCaller);
void j_transformation_cleanup(JTransformation*, gpointer, guint64, guint64,
			      JTransformationCaller);
JTransformationMode j_transformation_get_mode(JTransformation*);
JTransformationType j_transformation_get_type(JTransformation*);
gboolean j_transformation_need_whole_object(JTransformation*, JTransformationCaller);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JTransformation, j_transformation_unref)

G_END_DECLS

#endif
