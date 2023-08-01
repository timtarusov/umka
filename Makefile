
SHELL=/bin/bash

venv:  ## Set up virtual environment
	python3 -m venv venv
	venv/bin/pip install -r requirements.txt

install: venv
	unset CONDA_PREFIX && \
	source venv/bin/activate && maturin develop -m umka/Cargo.toml

install-release: venv
	unset CONDA_PREFIX && \
	source venv/bin/activate && maturin develop --release -m umka/Cargo.toml

build: venv
	unset CONDA_PREFIX && \
	source venv/bin/activate && maturin build --release -m umka/Cargo.toml

clean:
	-@rm -r venv
	-@cd src/umka-rs && cargo clean
