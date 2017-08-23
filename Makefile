.PHONY: docs-clean
docs-clean:
	rm -rf docs/source/apidocs/*
	rm -rf docs/build


.PHONY: docs
docs:
	sphinx-apidoc -o docs/source/apidocs -f cerise
	sphinx-build -a docs/source docs/build

