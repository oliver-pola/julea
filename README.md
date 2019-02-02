# JULEA

JULEA is a flexible storage framework that allows offering arbitrary client interfaces to applications.
To be able to rapidly prototype new approaches, it offers object and key-value backends that can either be client-side or server-side;
backends for popular storage technologies such as POSIX, LevelDB and MongoDB have already been implemented.

Additionally, JULEA allows dynamically adapting the I/O operations' semantics and can thus be adjusted to different use-cases.
It runs completely in user space, which eases development and debugging.
Its goal is to provide a solid foundation for storage research and teaching.

For more information, please refer to the [documentation](doc/README.md).

## Quick Start

To use JULEA, first clone the Git repository and enter the directory.

```console
$ git clone https://github.com/wr-hamburg/julea.git
$ cd julea
```

JULEA has two mandatory dependencies (GLib and libbson) and several optional ones that enable additional functionality.
The dependencies can either be installed using your operating system's package manage or with JULEA's script that installs them into the `dependencies` subdirectory using [Spack](https://spack.io/).

```console
$ ./scripts/install-dependencies.sh
```

After all dependencies have been installed, JULEA has to be configured and compiled using [Waf](https://waf.io/);
the different configuration and build options can be shown with `./waf --help`.
The `waf.sh` script is a wrapper around Waf that makes sure that the dependencies installed in the previous step are found.

```console
$ ./waf.sh configure --debug --sanitize
$ ./waf.sh build
```

To allow the shell to find JULEA's binaries and to set some variables useful for debugging, the environment has to be loaded.
Alternatively, the binaries can be found in the `build` directory.

```console
$ . ./scripts/environment.sh
```

Finally, a JULEA configuration has to be created.

```console
$ julea-config --user \
  --object-servers="$(hostname)" --kv-servers="$(hostname)" \
  --object-backend=posix --object-component=server --object-path=/tmp/julea \
  --kv-backend=leveldb --kv-component=server --kv-path=/tmp/julea
```

You can check whether JULEA works by executing the integrated test suite.

```console
$ ./scripts/test.sh
```

## Citing JULEA

If you want to cite JULEA, please use the following paper:

- [JULEA: A Flexible Storage Framework for HPC](https://link.springer.com/chapter/10.1007/978-3-319-67630-2_51) (Michael Kuhn), In High Performance Computing, Lecture Notes in Computer Science (10524), (Editors: Julian Kunkel, Rio Yokota, Michela Taufer, John Shalf), Springer International Publishing, ISC High Performance 2017, Frankfurt, Germany, ISBN: 978-3-319-67629-6, 2017-11
