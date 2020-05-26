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

#ifndef JULEA_TRANSFORMATION_CHUNKED_TRANSFORMATION_OBJECT_H
#define JULEA_TRANSFORMATION_CHUNKED_TRANSFORMATION_OBJECT_H

#if !defined(JULEA_TRANSFORMATION_H) && !defined(JULEA_TRANSFORMATION_COMPILATION)
#error "Only <julea-transformation.h> can be included directly."
#endif

#include <glib.h>
#include <julea.h>

G_BEGIN_DECLS

struct JChunkedTransformationObject;

typedef struct JChunkedTransformationObject JChunkedTransformationObject;

JChunkedTransformationObject* j_chunked_transformation_object_new(gchar const*, gchar const*);

JChunkedTransformationObject* j_chunked_transformation_object_new_for_index(guint32, gchar const*, gchar const*);
JChunkedTransformationObject* j_chunked_transformation_object_ref(JChunkedTransformationObject*);

void j_chunked_transformation_object_unref(JChunkedTransformationObject*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JChunkedTransformationObject, j_chunked_transformation_object_unref)

void j_chunked_transformation_object_create(JChunkedTransformationObject*, JBatch*, JTransformationType, JTransformationMode, guint64);
void j_chunked_transformation_object_delete(JChunkedTransformationObject*, JBatch*);

void j_chunked_transformation_object_read(JChunkedTransformationObject*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_chunked_transformation_object_write(JChunkedTransformationObject*, gpointer, guint64, guint64, guint64*, JBatch*);

void j_chunked_transformation_object_status(JChunkedTransformationObject*, gint64*, guint64*, JBatch*);
void j_chunked_transformation_object_status_ext(JChunkedTransformationObject*, gint64*, guint64*, guint64*, JTransformationType*, guint64*, guint64*, JBatch*);

G_END_DECLS

#endif
