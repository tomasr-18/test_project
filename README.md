0. Skapa en branch för micro-servicen
1. skapa följande filer:
    Dockerfile
    cloudbuild.yaml
2.  Skapa ett repo i Artifact repo i GCP
3.  Skapa en trigger under Cloud Run som triggas av updateringar på din branch
4.  Skapa en service under Cloud Run med den imagen som skapats i Cloud Run. imagen ska ligga i det Artifact Repo du precis skapade.


Kod som är deployad i GCP:
-fetch_news
-transform_news


Ordning på workflows:
fetch_news_ny
update_news_meta_data
clean_news_ny

--Linting localy--

cd .git/hooks
touch pre-push
chmod +x pre-push  # Gör filen körbar

echo '#!/bin/sh   #### SE TILL ATT ANVÄNDA RÄTT SHELL####
 
# List of directories to check
DIRS=("fetch_news" "fetch_stocks_raw" "to_ml_pipline" "transform_news_2" "transform_stocks")  # Add your folders here
 
# Run linting with Ruff on all specified directories
echo "Running linting with Ruff..."
for dir in "${DIRS[@]}"; do
    ruff check "$dir"
    LINT_RESULT=$?
    if [ $LINT_RESULT -ne 0 ]; then
        echo "Linting failed in directory $dir, aborting push."
        exit 1
    fi
done
 
# Run tests with pytest on all specified directories
echo "Running tests with pytest..."
for dir in "${DIRS[@]}"; do
    # Check if there are test files in the directory
    if ls "$dir"/test_*.py 1> /dev/null 2>&1; then
        pytest "$dir"
        TEST_RESULT=$?
        if [ $TEST_RESULT -ne 0 ]; then
            echo "Tests failed in directory $dir, aborting push."
            exit 1
        fi
    else
        echo "No test files found in directory $dir, skipping tests."
    fi
done
 
echo "Linting and tests passed for all directories, proceeding with push."
exit 0' > pre-push

