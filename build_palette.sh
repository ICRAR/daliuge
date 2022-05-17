wget https://raw.githubusercontent.com/ICRAR/daliuge/master/tools/xml2palette/xml2palette.py
export PROJECT_NAME=$(basename -s .git `git config --get remote.origin.url`)
export PROJECT_VERSION=$(git rev-parse --short HEAD)
export GIT_REPO=$(git config --get remote.origin.url)
python3 xml2palette.py -i ./ -o ${PROJECT_NAME}.palette
rm xml2palette.py