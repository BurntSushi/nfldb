docs:
	scripts/generate-docs
	rsync -rh --inplace \
		doc/* \
		Geils:~/www/burntsushi.net/public_html/doc/nfldb/

pypi: docs
	sudo python2 setup.py register sdist upload

pypi-meta:
	python2 setup.py register

pep8:
	pep8-python2 nfldb/*.py
	pep8-python2 scripts/nfldb-update

push:
	git push origin master
	git push github master
