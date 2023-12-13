if curl "http://base-${OS_GIT_MAJOR}-${OS_GIT_MINOR}.ocp${ARCH_SUFFIX}.svc" > /etc/yum.repos.d/art.repo ; then
	echo "Injected ART repos"
	cat /etc/yum.repos.d/art.repo
else
	echo "Unable to inject ART CI repo mirror yum configuration. This is expected if you are building locally. If" \
	"this is a CI triggered build, contact @release-artists in #forum-ocp-art ."
fi
