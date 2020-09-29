/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2020 Michael Kuhn
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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <julea.h>
#include <julea-transformation.h>

#include "benchmark.h"

static void
_benchmark_transformation_object_create(BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 100000;

	g_autoptr(JBatch) delete_batch = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	delete_batch = j_batch_new(semantics);
	batch = j_batch_new(semantics);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JTransformationObject) object = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		object = j_transformation_object_new("benchmark", name);
		j_transformation_object_create(object, batch, J_TRANSFORMATION_TYPE_LZ4, J_TRANSFORMATION_MODE_CLIENT);

		j_transformation_object_delete(object, delete_batch);

		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}
	}

	if (use_batch)
	{
		ret = j_batch_execute(batch);
		g_assert_true(ret);
	}

	elapsed = j_benchmark_timer_elapsed();

	ret = j_batch_execute(delete_batch);
	g_assert_true(ret);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static void
benchmark_transformation_object_create(BenchmarkResult* result)
{
	_benchmark_transformation_object_create(result, FALSE);
}

static void
benchmark_transformation_object_create_batch(BenchmarkResult* result)
{
	_benchmark_transformation_object_create(result, TRUE);
}

static void
_benchmark_transformation_object_delete(BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 100000;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JTransformationObject) object = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		object = j_transformation_object_new("benchmark", name);
		j_transformation_object_create(object, batch, J_TRANSFORMATION_TYPE_LZ4, J_TRANSFORMATION_MODE_CLIENT);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JTransformationObject) object = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		object = j_transformation_object_new("benchmark", name);

		j_transformation_object_delete(object, batch);

		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}
	}

	if (use_batch)
	{
		ret = j_batch_execute(batch);
		g_assert_true(ret);
	}

	elapsed = j_benchmark_timer_elapsed();

	result->elapsed_time = elapsed;
	result->operations = n;
}

static void
benchmark_transformation_object_delete(BenchmarkResult* result)
{
	_benchmark_transformation_object_delete(result, FALSE);
}

static void
benchmark_transformation_object_delete_batch(BenchmarkResult* result)
{
	_benchmark_transformation_object_delete(result, TRUE);
}

static void
_benchmark_transformation_object_status(BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 200000;

	g_autoptr(JTransformationObject) object = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gchar dummy[1];
	gdouble elapsed;
	gint64 modification_time;
	guint64 size;
	gboolean ret;

	memset(dummy, 0, 1);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	object = j_transformation_object_new("benchmark", "benchmark");
	j_transformation_object_create(object, batch, J_TRANSFORMATION_TYPE_LZ4, J_TRANSFORMATION_MODE_CLIENT);
	j_transformation_object_write(object, dummy, 1, 0, &size, batch);

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		j_transformation_object_status(object, &modification_time, &size, batch);

		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}
	}

	if (use_batch)
	{
		ret = j_batch_execute(batch);
		g_assert_true(ret);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_transformation_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static void
benchmark_transformation_object_status(BenchmarkResult* result)
{
	_benchmark_transformation_object_status(result, FALSE);
}

static void
benchmark_transformation_object_status_batch(BenchmarkResult* result)
{
	_benchmark_transformation_object_status(result, TRUE);
}

static void
_benchmark_transformation_object_read(BenchmarkResult* result, gboolean use_batch, guint block_size)
{
	guint const n = 5000;

	g_autoptr(JTransformationObject) object = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gchar dummy[block_size];
	gdouble elapsed;
	guint64 nb = 0;
	gboolean ret;

	memset(dummy, 0, block_size);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	object = j_transformation_object_new("benchmark", "benchmark");
	j_transformation_object_create(object, batch, J_TRANSFORMATION_TYPE_LZ4, J_TRANSFORMATION_MODE_CLIENT);

	for (guint i = 0; i < n; i++)
	{
		j_transformation_object_write(object, dummy, block_size, i * block_size, &nb, batch);
	}

	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		j_transformation_object_read(object, dummy, block_size, i * block_size, &nb, batch);


		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}
	}

	if (use_batch)
	{
		ret = j_batch_execute(batch);
		g_assert_true(ret);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_transformation_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	result->elapsed_time = elapsed;
	result->operations = n;
	result->bytes = n * block_size;
}

static void
benchmark_transformation_object_read(BenchmarkResult* result)
{
	_benchmark_transformation_object_read(result, FALSE, 4 * 1024);
}

static void
benchmark_transformation_object_read_batch(BenchmarkResult* result)
{
	_benchmark_transformation_object_read(result, TRUE, 4 * 1024);
}

static void
_benchmark_transformation_object_write(BenchmarkResult* result, gboolean use_batch, guint block_size)
{
	guint const n = 5000;

	g_autoptr(JTransformationObject) object = NULL;
	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gchar dummy[block_size];
	gdouble elapsed;
	guint64 nb = 0;
	gboolean ret;

	memset(dummy, 0, block_size);

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	object = j_transformation_object_new("benchmark", "benchmark");
	j_transformation_object_create(object, batch, J_TRANSFORMATION_TYPE_LZ4, J_TRANSFORMATION_MODE_CLIENT);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		j_transformation_object_write(object, &dummy, block_size, i * block_size, &nb, batch);

		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}
	}

	if (use_batch)
	{
		ret = j_batch_execute(batch);
		g_assert_true(ret);
	}

	elapsed = j_benchmark_timer_elapsed();

	j_transformation_object_delete(object, batch);
	ret = j_batch_execute(batch);
	g_assert_true(ret);

	result->elapsed_time = elapsed;
	result->operations = n;
	result->bytes = n * block_size;
}

static void
benchmark_transformation_object_write(BenchmarkResult* result)
{
	_benchmark_transformation_object_write(result, FALSE, 4 * 1024);
}

static void
benchmark_transformation_object_write_batch(BenchmarkResult* result)
{
	_benchmark_transformation_object_write(result, TRUE, 4 * 1024);
}

static void
_benchmark_transformation_object_unordered_create_delete(BenchmarkResult* result, gboolean use_batch)
{
	guint const n = 100000;

	g_autoptr(JBatch) batch = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;
	gboolean ret;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);

	j_benchmark_timer_start();

	for (guint i = 0; i < n; i++)
	{
		g_autoptr(JTransformationObject) object = NULL;
		g_autofree gchar* name = NULL;

		name = g_strdup_printf("benchmark-%d", i);
		object = j_transformation_object_new("benchmark", name);
		j_transformation_object_create(object, batch, J_TRANSFORMATION_TYPE_LZ4, J_TRANSFORMATION_MODE_CLIENT);

		j_transformation_object_delete(object, batch);

		if (!use_batch)
		{
			ret = j_batch_execute(batch);
			g_assert_true(ret);
		}
	}

	if (use_batch)
	{
		ret = j_batch_execute(batch);
		g_assert_true(ret);
	}

	elapsed = j_benchmark_timer_elapsed();

	result->elapsed_time = elapsed;
	result->operations = n * 2;
}

void
benchmark_transformation(void)
{
	j_benchmark_run("/transformation/transformation-object/create", benchmark_transformation_object_create);
	j_benchmark_run("/transformation/transformation-object/create-batch", benchmark_transformation_object_create_batch);
	j_benchmark_run("/transformation/transformation-object/delete", benchmark_transformation_object_delete);
	j_benchmark_run("/transformation/transformation-object/delete-batch", benchmark_transformation_object_delete_batch);
	j_benchmark_run("/transformation/transformation-object/status", benchmark_transformation_object_status);
	j_benchmark_run("/transformation/transformation-object/status-batch", benchmark_transformation_object_status_batch);
	/* FIXME get */
	j_benchmark_run("/transformation/transformation-object/read", benchmark_transformation_object_read);
	j_benchmark_run("/transformation/transformation-object/read-batch", benchmark_transformation_object_read_batch);
	j_benchmark_run("/transformation/transformation-object/write", benchmark_transformation_object_write);
	j_benchmark_run("/transformation/transformation-object/write-batch", benchmark_transformation_object_write_batch);
}
