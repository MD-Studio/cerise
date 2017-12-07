.PHONY: docs-clean
docs-clean:
	rm -rf docs/source/apidocs/*
	rm -rf docs/build


.PHONY: docs
docs:
	sphinx-build -a docs/source docs/build

.PHONY: test
test:
	pytest --cov
	coverage combine --append integration_test
	coverage xml
	coverage report
