docs:
	scripts/generate-docs
	rsync -rh --inplace \
		doc/* \
		Geils:~/www/burntsushi.net/public_html/doc/nflvid/

pypi: docs
	sudo python2 setup.py register sdist upload

pypi-meta:
	python2 setup.py register

pep8:
	pep8-python2 nflvid/*.py
	pep8-python2 scripts/{download-all-pbp-xml,nflvid-footage,nflvid-slice}

push:
	git push origin master
	git push github master
