.PHONY: docs-clean
docs-clean:
	rm -rf docs/source/apidocs/*
	rm -rf docs/build


.PHONY: docs
docs:
	sphinx-build -a docs/source docs/build

.PHONY: test
test:
	pytest --cov --ignore=docs
	coverage combine --append integration_test
	coverage xml
	coverage report -m


.PHONY: fast_test
fast_test:
	pytest --cov --ignore=docs --ignore=integration_test -x
	coverage report -m
