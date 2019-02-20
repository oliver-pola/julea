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

#ifndef JULEA_OBJECT_TRANSFORMATION_OBJECT_H
#define JULEA_OBJECT_TRANSFORMATION_OBJECT_H

#if !defined(JULEA_OBJECT_H) && !defined(JULEA_OBJECT_COMPILATION)
#error "Only <julea-object.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>

G_BEGIN_DECLS

struct JTransformationObject;

typedef struct JTransformationObject JTransformationObject;

JTransformationObject* j_transformation_object_new (gchar const*, gchar const*);

JTransformationObject* j_transformation_object_new_for_index (guint32, gchar const*, gchar const*);
JTransformationObject* j_transformation_object_ref (JTransformationObject*);

void j_transformation_object_unref (JTransformationObject*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JTransformationObject, j_transformation_object_unref)

void j_transformation_object_create (JTransformationObject*, JBatch*, JTransformationType, JTransformationMode, void*);
void j_transformation_object_delete (JTransformationObject*, JBatch*);

void j_transformation_object_read (JTransformationObject*, gpointer, guint64, guint64, guint64*, JBatch*);
void j_transformation_object_write (JTransformationObject*, gconstpointer, guint64, guint64, guint64*, JBatch*);

void j_transformation_object_status (JTransformationObject*, gint64*, guint64*, JBatch*);

G_END_DECLS

#endif
