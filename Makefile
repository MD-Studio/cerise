.PHONY: docs-clean
docs-clean:
	rm -rf docs/source/apidocs/*
	rm -rf docs/build


.PHONY: docs
docs:
	sphinx-build -a docs/source docs/build

.PHONY: test
test:
	pytest --cov --cov-report xml --cov-report term-missing --ignore=docs
	mypy cerise


.PHONY: fast_test
fast_test:
	pytest --cov --ignore=docs --ignore=setup.py --ignore=api -k 'not test_service' -x
	mypy cerise
	coverage report -m
