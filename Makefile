ENV=$(VIRTUAL_ENV)/bin

upload:
	$(ENV)/python setup.py sdist register upload
	rm -rf dist

test:
	@cd tests; PYTHONPATH=.. py.test
