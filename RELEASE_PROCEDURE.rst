Release procedure
=================

* Make sure you are in the master branch, with the latest changes,
  and a clean copy
  ::

    git checkout master
    git pull
    git status


* Decide on the next version number, making reference to Semantic Versioning

* Update the Changelog with:
  * The new version as a heading
  * List each merged PRs that have been committed to master since the last Release with a copy of the PR link

* Run

  :: 

    make release


* This will:

  * Confirm that you have updated the changelog with the new version information
  * Update the VERSION files across the repo for each module 
  * Tag and commit all files
  * Push everything to master

* Using the GitHub UI to create an official release: 

  * Navigate to the Releases section in the right-hand side
  * Click "Draft a new Release" 
  * Choose the most recent tagged version that you just created and click "Generate release notes" 
  * Click "Publish Release"

* If everything is successful, this will push to PyPI and DockerHub!
