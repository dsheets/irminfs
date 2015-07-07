.PHONY: all clean

all:
	ocamlbuild -use-ocamlfind cli/irminfs_main.native

clean:
	ocamlbuild -clean
