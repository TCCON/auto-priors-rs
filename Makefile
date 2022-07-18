PY_SITE_DIR=$(shell python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")

help:
	@echo "Available targets:"
	@echo "  * install-python-bindings: Copy the python bindings into a package in $(PY_SITE_DIR)"

install-python-bindings: cargo-build
	rm -rf $(PY_SITE_DIR)/orm_bindings
	mkdir $(PY_SITE_DIR)/orm_bindings
	cp target/debug/libpy_ginput_bindings.so $(PY_SITE_DIR)/orm_bindings/py_ginput_bindings.so
	echo "from .py_ginput_bindings import *" > $(PY_SITE_DIR)/orm_bindings/__init__.py

cargo-build:
	cargo build