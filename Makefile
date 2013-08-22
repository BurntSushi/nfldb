all:
	@echo "Specify a target."

docs:
	pdoc --html --html-dir ./doc --overwrite ./nfldb

pypi: docs
	sudo python2 setup.py register sdist upload

longdesc.rst: nfldb/__init__.py docstring
	pandoc -f markdown -t rst -o longdesc.rst docstring
	rm -f docstring

docstring: nfldb/__init__.py
	./extract-docstring > docstring

dev-install: docs
	[[ -n "$$VIRTUAL_ENV" ]] || exit
	rm -rf ./dist
	python setup.py sdist
	pip install -U dist/*.tar.gz

pep8:
	pep8-python2 nfldb/{__init__,db,types,version}.py
	pep8-python2 scripts/nfldb-update

push:
	git push origin master
	git push github master
