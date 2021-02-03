Release procedure
=================

* Make sure you are in the master branch, with the latest changes,
  and a clean copy
  ::

    git checkout master
    git pull
    git status


* Decide a new version number (with major, minor, patch components) and apply it
  in all packages' ``setup.py``
  ::

    VERSION_MAJOR=X
    VERSION_MINOR=Y
    VERSION_PATCH=Z
    VERSION=$VERSION_MAJOR.$VERSION_MINOR.$VERSION_PATCH
    SETUP_FILES="daliuge-common/setup.py daliuge-translator/setup.py daliuge-engine/setup.py setup.py"
    sed -i "s/MAJOR = .*/MAJOR = $VERSION_MAJOR/; s/MINOR = .*/MINOR = $VERSION_MINOR/; s/PATCH = .*/PATCH = $VERSION_PATCH/" $SETUP_FILES

* Double-check these are the only changes you will commit
  ::

    git diff

* Commit the version changes
  ::

    git commit $SETUP_FILES -m "daliuge $VERSION"

* Tag the repository with a new version name tag (using ``vX.Y.Z`` format)
  ::

    git tag v$VERSION -m "daliuge $VERSION"
    # Alternatively, if you want to sign it
    git tag v$VERSION -m "daliuge $VERSION" -s

* Push the new commit and the tag to GitHub
  ::

    git push origin master v$VERSION

* Produce and collect source distributions for all packages
  ::

    python setup.py sdist
    for package in daliuge-common daliuge-translator daliuge-engine; do
      cd $package
      python setup.py sdist
      cp dist/*-$VERSION.tar.gz $OLDPWD/dist
      cd $OLDPWD
    done

* Upload to PyPI 
  ::

    pip install twine
    # Adjust credentials in ~/.pypirc, then
    twine upload dist/daliuge*-$VERSION.tar.gz
