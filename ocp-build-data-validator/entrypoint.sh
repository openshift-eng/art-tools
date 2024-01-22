# get pr_api_url from pr_html_url
pr=`echo $PR_URL | sed 's/github.com/api.github.com\/repos/g'| sed 's/pull/pulls/g'`
# clone forked branch
git clone --branch `curl -s $pr | jq -r .head.ref` --single-branch --depth=1  `curl -s $pr | jq -r .head.repo.html_url`
# cd to repo
cd `curl -s $pr| jq -r .head.repo.name`
# validate changed files and output to result.output
curl -s $pr/files | jq -r .[].filename | xargs validate-ocp-build-data -s $1 2>&1 | tee result.output
out=""
# loop result.output into $out
while IFS= read -r line ; do out="$out$line\r\n"; done < result.output
# adjust format for json
out=$(echo $out | sed 's/"/\\"/g')
# add comment with result to pr
curl -L -X POST -H "Authorization: token $GITHUB_TOKEN" `curl -s $pr | jq -r ._links.comments.href` -d "{\"body\":\"$out\"}"
