.PHONY: docs-clean
docs-clean:
	rm -rf docs/build


.PHONY: docs
docs:
	sphinx-apidoc -o docs/source -f cerise
	sphinx-build -a docs/source docs/build

