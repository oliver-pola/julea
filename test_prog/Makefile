
all:
	gcc -Wall  -I../include test_prog.c -o test `pkg-config --cflags --libs ./julea.pc` -L../dependencies/opt/spack/linux-arch-x86_64/gcc-8.2.1/glib-2.56.2-cbgvs2t64kxvrwl7wfbtrlt6l567qfho/lib -lglib-2.0 -L../build/lib -ljulea-object
clean:
	rm ./test
