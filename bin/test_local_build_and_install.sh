#!/bin/bash

restore() {
    echo
    echo "Restore setup.py and requirements.txt files from git repository..."
    git checkout -- setup.py
    git checkout -- requirements.txt
    echo "Clean-up any build files..."
    rm -rf build src/python/*.egg-info
}

# always execute restore function before script ends either by exit call or
# through Ctrl-C (shell interrupt)
trap restore EXIT
trap restore SIGINT

set -e  # Exit on any error

# Define package list
packages=("wmagent" "wmagentdev" "wmcore" "reqmon" "reqmgr2" "wmglobalqueue" "acdcserver" \
          "msunmerged" "msoutput" "mspileup" "msrulecleaner" \
          "mstransferor" "msmonitor")

if [ -d venv ]; then
    echo "WARNING: Found venv area, please either delete or relocate it to proceed..."
    exit 1
fi

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Upgrade build tools
pip install --upgrade pip setuptools wheel

# extract python version
pyver=`python -V | awk '{print $2}' | cut -d'.' -f1,2`
export PYTHONPATH=$PYTHONPATH:$PWD:$PWD/venv/lib/python$pyver/site-packages
echo
echo "PYTHONPATH=$PYTHONPATH"

# preserve setup.py and requirements.txt files we they will be overwritten
# below for every WM package we build
echo
echo "### Preserving setup.py and requirements.txt files..."
# first restore these files from git
restore
# now let's make local copy to use in our builds
cp setup.py setup.py.orig
cp requirements.txt requirements.txt.orig

# Loop through each package
for pkg in "${packages[@]}"; do
    echo
    echo "### Building and testing package: $pkg ..."
    sleep 2

    # Clean previous builds
    python3 setup.py clean
    rm -rf dist build *.egg-info

    # copy original files
    /bin/cp -f setup.py.orig setup.py
    /bin/cp -f requirements.txt.orig requirements.txt

    # Update setup.py
    sed "s/PACKAGE_TO_BUILD/${pkg}/" setup_template.py > setup.py

    # Define temporary requirements file
    TMP_REQUIREMENTS="/tmp/requirements_${pkg}.txt"

    # Create package-specific requirements file
    # NOTE: we will skip gfal and htcondor (on macOS) due to their special nature
    # to avoid dependency failure
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # no htcondor on macos, we will skip it during test
        cat requirements.txt | egrep -v "htcondor|gfal" > requirements.$pkg.txt
    else
        cat requirements.txt | egrep -v "gfal" > requirements.$pkg.txt
    fi
    awk "/(${pkg}$)|(${pkg},)/ {print \$1}" requirements.$pkg.txt > requirements.txt

    # Build package
    python3 setup.py sdist bdist_wheel

    # verify dist area
    ls -lh dist/

    # Install package locally
    pip install -r requirements.txt
    pip install --no-index --find-links=dist/ ${pkg}

    # installed package installation
    installed=`pip list | grep $pkg | awk '{print $1}'`
    echo
    if [ "$installed" == "$pkg" ]; then
        echo "### Successfully built and installed: ${pkg} ..."
        rm requirements.$pkg.txt
        /bin/cp -f setup.py.orig setup.py
        /bin/cp -f requirements.txt.orig requirements.txt
        sleep 2
    else
        echo "ERROR: fail to install $pkg ..."
        exit 1
    fi
done

# now list all installed packages
echo
echo "### List installed WM packages:"
pip list --format=freeze | grep -E "^($(IFS='|'; echo "${packages[*]}"))="

echo
echo "SUCCESS: All packages are built and installed successfully..."
