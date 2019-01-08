#include <glib.h>
#include <julea.h>
#include <julea-object.h>
#include <stdio.h>

int main(int argc, char** argv)
{
    j_init();
    printf("JULEA initialized\n");

    g_autoptr(JSemantics) semantics = NULL;
    g_autoptr(JBatch) batch = NULL;
    g_autoptr(JBatch) delete_batch = NULL;
    g_autoptr(JObject) object = NULL;

    semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_POSIX);
    batch = j_batch_new(semantics);
    delete_batch = j_batch_new(semantics);

    g_autofree gchar* name = NULL;
    name = g_strdup_printf("testobject");
    object = j_object_new("test", name);

    // Create an Object
    j_object_create(object, batch);
    j_batch_execute(batch);

    // Creating dummy data
    gchar data_block[10];
    memset(data_block, 65, 10);

    guint64 bytes_written = 0;

    g_autoptr(JBatch) write_batch = NULL;
    write_batch = j_batch_new(semantics);

    // Do the write
    j_object_write(object, data_block, 10, 0, &bytes_written, write_batch);
    j_batch_execute(write_batch);
    printf("Number of bytes written to object: %ld\n", bytes_written);

    g_autoptr(JBatch) status_batch = NULL;
    status_batch = j_batch_new(semantics);

    gint64 mod_time = 0;
    guint64 size = 0;

    j_object_status(object, &mod_time, &size, status_batch);
    j_batch_execute(status_batch);

    printf("Object Status:\n Modification time: %ld\n Size: %ld\n", mod_time, size);

    // Read from the object byte per byte and print the read data
    guint64 bytes_read = 0;
    for(guint i = 0; i < 10; i++)
    {
        g_autoptr(JBatch) read_batch = NULL;
        read_batch = j_batch_new(semantics);

        gchar buff[10];

        j_object_read(object, &buff[i], 1, i, &bytes_read, read_batch);
        j_batch_execute(read_batch);

        printf("Read #%d, value: %c, bytes read: %ld\n", i, buff[i], bytes_read);
    }


    // Delete the Object 
    j_object_delete(object, delete_batch);
    j_batch_execute(delete_batch);


    j_fini();
    printf("JULEA stopped\n");
}
