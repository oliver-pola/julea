/*
 * Copyright (c) 2017 Michael Kuhn
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHORS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <jbackend.h>
#include <jbackend-internal.h>

#include <jtrace-internal.h>

/**
 * \defgroup JHelper Helper
 *
 * Helper data structures and functions.
 *
 * @{
 **/

static
GModule*
j_backend_load (gchar const* name, gchar const* component, JBackendType type, JBackend** backend)
{
	JBackend* (*module_backend_info) (JBackendType) = NULL;

	JBackend* tmp_backend = NULL;
	GModule* module = NULL;
	gchar* cpath = NULL;
	gchar* path = NULL;

#ifdef JULEA_BACKEND_PATH_BUILD
	cpath = g_build_filename(JULEA_BACKEND_PATH_BUILD, component, NULL);
	path = g_module_build_path(cpath, name);
	module = g_module_open(path, G_MODULE_BIND_LOCAL);
	g_free(cpath);
	g_free(path);
#endif

	if (module == NULL)
	{
		cpath = g_build_filename(JULEA_BACKEND_PATH, component, NULL);
		path = g_module_build_path(cpath, name);
		module = g_module_open(path, G_MODULE_BIND_LOCAL);
		g_free(cpath);
		g_free(path);
	}

	if (module != NULL)
	{
		g_module_symbol(module, "backend_info", (gpointer*)&module_backend_info);

		g_assert(module_backend_info != NULL);

		tmp_backend = module_backend_info(type);

		if (tmp_backend != NULL)
		{
			g_assert(tmp_backend->type == type);

			if (type == J_BACKEND_TYPE_DATA)
			{
				g_assert(tmp_backend->u.data.init != NULL);
				g_assert(tmp_backend->u.data.fini != NULL);
				g_assert(tmp_backend->u.data.create != NULL);
				g_assert(tmp_backend->u.data.delete != NULL);
				g_assert(tmp_backend->u.data.open != NULL);
				g_assert(tmp_backend->u.data.close != NULL);
				g_assert(tmp_backend->u.data.status != NULL);
				g_assert(tmp_backend->u.data.sync != NULL);
				g_assert(tmp_backend->u.data.read != NULL);
				g_assert(tmp_backend->u.data.write != NULL);
			}
			else if (type == J_BACKEND_TYPE_META)
			{
				g_assert(tmp_backend->u.meta.init != NULL);
				g_assert(tmp_backend->u.meta.fini != NULL);
				g_assert(tmp_backend->u.meta.create != NULL);
				g_assert(tmp_backend->u.meta.delete != NULL);
				g_assert(tmp_backend->u.meta.get != NULL);
				g_assert(tmp_backend->u.meta.get_all != NULL);
				g_assert(tmp_backend->u.meta.iterate != NULL);
			}
		}
	}

	*backend = tmp_backend;

	return module;
}

GModule*
j_backend_load_client (gchar const* name, JBackendType type, JBackend** backend)
{
	return j_backend_load(name, "client", type, backend);
}

GModule*
j_backend_load_server (gchar const* name, JBackendType type, JBackend** backend)
{
	return j_backend_load(name, "server", type, backend);
}

/**
 * @}
 **/
