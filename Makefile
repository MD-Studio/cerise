.PHONY: docs-clean
docs-clean:
	rm -rf docs/build


.PHONY: docs
docs:
	sphinx-apidoc -o docs/source -f simple_cwl_xenon_service
	sphinx-build -a docs/source docs/build

