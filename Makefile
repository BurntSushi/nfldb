REMOTE=Geils:~/www/burntsushi.net/public_html/stuff/nfldb/

all:
	@echo "Specify a target."

pypi: docs er sql longdesc.rst
	sudo python2 setup.py register sdist upload

docs: er
	pdoc --html --html-dir ./doc --overwrite ./nfldb

er:
	nfldb-write-erwiz > /tmp/nfldb.erwiz
	erwiz /tmp/nfldb.erwiz doc/nfldb.pdf
	erwiz /tmp/nfldb.erwiz doc/nfldb.png

	nfldb-write-erwiz --condense > /tmp/nfldb-condensed.erwiz
	erwiz /tmp/nfldb-condensed.erwiz doc/nfldb-condensed.pdf
	erwiz /tmp/nfldb-condensed.erwiz doc/nfldb-condensed.png

	rsync doc/nfldb*{pdf,png} $(REMOTE)

sql:
	./scripts/nfldb-dump /tmp/nfldb.sql
	zip /tmp/nfldb.sql.zip /tmp/nfldb.sql
	rsync --progress /tmp/nfldb.sql.zip $(REMOTE)
	rm -f /tmp/nfldb.{sql,sql.zip}

longdesc.rst: nfldb/__init__.py docstring
	pandoc -f markdown -t rst -o longdesc.rst docstring
	rm -f docstring

docstring: nfldb/__init__.py
	./scripts/extract-docstring > docstring

dev-install:
	[[ -n "$$VIRTUAL_ENV" ]] || exit
	rm -rf ./dist
	python setup.py sdist
	pip install -U dist/*.tar.gz

pep8:
	pep8-python2 nfldb/{__init__,db,query,types,version}.py
	pep8-python2 scripts/{nfldb-update,nfldb-write-erwiz}

push:
	git push origin master
	git push github master
