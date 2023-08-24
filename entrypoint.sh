BRANCH="automated-submodule-update"
MOD_NAME="art-tools"
MOD_ORG="openshift-eng"
MOD_BRANCH="main"
GITHUB_REPOSITORY="openshift-eng/aos-cd-jobs"
GITHUB_REPOSITORY_OWNER="openshift-bot"

API_HEADER="Accept: application/vnd.github.v3+json"
AUTH_HEADER="Authorization: token $GITHUB_TOKEN"
URI="https://api.github.com"
user_response=$(curl -s -H "${AUTH_HEADER}" -H "$API_HEADER" "$URI/users/$GITHUB_REPOSITORY_OWNER")

# Fetch the latest sha for the art-tools branch from that repo
mod=$(curl -s -H "$AUTH_HEADER" -H "${API_HEADER}" "$URI/repos/$GITHUB_REPOSITORY/contents/$MOD_NAME")

mod_remote=$(curl -s -H "$AUTH_HEADER" -H "$API_HEADER" "$URI/repos/$MOD_ORG/$MOD_NAME/git/ref/heads/$MOD_BRANCH")
sha_remote=$(echo "$mod_remote" | jq -r .object.sha)

# Is the new ref the same as the current one?
if [[ "$sha_remote" == $(echo "$mod" | jq -r .sha) ]]; then
    echo "This submodule is already up to date"
    exit 0
fi
echo "master repo's submodule is outdated"

# Is it the same as the ref on the PR branch?
mod_pr=$(curl -s -H "$AUTH_HEADER" -H "$API_HEADER" "$URI/repos/$GITHUB_REPOSITORY/contents/$MOD_NAME?ref=$BRANCH")
sha_branch=$(echo "$mod_pr" | jq -r .sha)
if [[ "$sha_branch" != "null" ]]; then
    if [[ "$sha_remote" == "$sha_branch" ]]; then
        echo "This submodule is already updated on the PR branch"
        exit 0
    else
        curl -s -X DELETE -H "$AUTH_HEADER" -H "$API_HEADER" "$URI/repos/$GITHUB_REPOSITORY/git/refs/heads/$BRANCH"
    fi
fi

# Create a PR with this commit hash if it doesn't exist
echo "Create new pr to update submodule"
B64_PAT=$(printf "%s""pat:$GITHUB_TOKEN" | base64)
git clone -b master "https://github.com/$GITHUB_REPOSITORY"
cd ./aos-cd-jobs
git config --local http.https://github.com.extraheader "AUTHORIZATION: basic $B64_PAT"
git config --local user.email $(echo "$user_response" | jq -r ".email")
git config --local user.name $(echo "$user_response" | jq -r ".login")

git checkout -b $BRANCH
git submodule update --init --recursive --remote -- $MOD_NAME
git commit -am "Update [$MOD_NAME] submodule to [$MOD_ORG/$MOD_NAME@$MOD_BRANCH]"
git push --set-upstream origin $BRANCH

curl -L -X POST -H "${AUTH_HEADER}" -H "$API_HEADER" "$URI/repos/$GITHUB_REPOSITORY/pulls" \
     -d "{\"title\":\"Automated submodule update [$MOD_NAME]\",\"body\":\"Update [$MOD_NAME] submodule to [$MOD_ORG/$MOD_NAME@$MOD_BRANCH]\",\"head\":\"$BRANCH\",\"base\":\"master\"}"
