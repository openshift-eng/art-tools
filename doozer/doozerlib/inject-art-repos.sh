if curl http://base-$major-$minor.ocp$arch_suffix.svc > /etc/yum.repos.d/art.repo ; then
	echo "Injected ART repos"
	cat /etc/yum.repos.d/art.repo
else
	echo "Unable to inject ART CI repo mirror yum configuration. This is expected if you are building locally. If this is a CI triggered build, contact @release-artists in #aos-art ."
fi
