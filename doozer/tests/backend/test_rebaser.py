from unittest import TestCase
from unittest.mock import MagicMock
from pathlib import Path
from dockerfile_parse import DockerfileParser
from artcommonlib.model import Missing
from doozerlib.backend.rebaser import KonfluxRebaser


class TestRebaser(TestCase):
    def test_split_dockerfile_into_stages_1(self):
        dfp = DockerfileParser()
        dfp.content = """
            FROM base
            LABEL foo="bar baz"
            USER 1000
            FROM base2
            USER 2000
            RUN commands
        """

        actual = KonfluxRebaser.split_dockerfile_into_stages(dfp)
        expected = [[{'FROM': 'base'}, {'LABEL': 'foo="bar baz"'}, {'USER': '1000'}], [{'FROM': 'base2'}, {'USER': '2000'}, {'RUN': 'commands'}]]

        self.assertEqual(len(actual), 2)
        self.assertEqual(actual, expected)

    def test_split_dockerfile_into_stages_2(self):
        dfp = DockerfileParser()
        dfp.content = """
            FROM base
            LABEL foo="bar baz"
            USER 1000
        """

        actual = KonfluxRebaser.split_dockerfile_into_stages(dfp)
        expected = [[{'FROM': 'base'}, {'LABEL': 'foo="bar baz"'}, {'USER': '1000'}]]

        self.assertEqual(len(actual), 1)
        self.assertEqual(actual, expected)

    def test_add_build_repos_1(self):
        """
        Test with defalut values, and has non-USER 0 lines
        """
        metadata = MagicMock()
        metadata.config.konflux.network_mode = Missing
        metadata.config.konflux.cachito.mode = Missing
        metadata.config.final_stage_user = Missing

        dfp = DockerfileParser()
        dfp.content = """
FROM base1
LABEL foo="bar baz"
USER 1000
FROM base2
USER 2000
RUN commands
               """
        expected = """
FROM base1

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp
# End Konflux-specific steps
USER 2000
RUN commands


# Start Konflux-specific steps
USER 0
RUN cp /tmp/yum_temp/* /etc/yum.repos.d/ || true
USER 2000
# End Konflux-specific steps
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path("."))

        self.assertEqual(dfp.content, expected)

    def test_add_build_repos_2(self):
        """
        Test with default values, but with final_stage_user set
        """
        metadata = MagicMock()
        metadata.config.konflux.network_mode = Missing
        metadata.config.konflux.cachito.mode = Missing
        metadata.config.final_stage_user = "USER 3000"

        dfp = DockerfileParser()
        dfp.content = """
FROM base1
LABEL foo="bar baz"
USER 1000
FROM base2
USER 2000
RUN commands
               """
        expected = """
FROM base1

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp
# End Konflux-specific steps
USER 2000
RUN commands


# Start Konflux-specific steps
USER 0
RUN cp /tmp/yum_temp/* /etc/yum.repos.d/ || true
USER 3000
# End Konflux-specific steps
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path("."))

        self.assertEqual(dfp.content, expected)

    def test_add_build_repos_3(self):
        """
        Test with network_mode hermetic
        """
        metadata = MagicMock()
        metadata.config.konflux.network_mode = "hermetic"
        metadata.config.konflux.cachito.mode = Missing
        metadata.config.final_stage_user = "USER 3000"

        dfp = DockerfileParser()
        dfp.content = """
FROM base1
LABEL foo="bar baz"
USER 1000
FROM base2
USER 2000
RUN commands
               """
        expected = """
FROM base1

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=hermetic
ENV ART_BUILD_DEPS_MODE=default
# End Konflux-specific steps
LABEL foo="bar baz"
USER 1000
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=hermetic
ENV ART_BUILD_DEPS_MODE=default
# End Konflux-specific steps
USER 2000
RUN commands
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path("."))
        dfp.content.strip()
        self.assertEqual(expected.strip(), dfp.content.strip())

    def test_add_build_repos_4(self):
        """
        Test with non-hermetic, but with final_stage_user
        """
        metadata = MagicMock()
        metadata.config.konflux.network_mode = Missing
        metadata.config.konflux.cachito.mode = Missing
        metadata.config.final_stage_user = "USER 3000"

        dfp = DockerfileParser()
        dfp.content = """
FROM base1
LABEL foo="bar baz"
FROM base2
RUN commands
               """
        expected = """
FROM base1

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp
# End Konflux-specific steps
LABEL foo="bar baz"
FROM base2

# Start Konflux-specific steps
ENV ART_BUILD_ENGINE=konflux
ENV ART_BUILD_DEPS_METHOD=cachi2
ENV ART_BUILD_NETWORK=open
ENV ART_BUILD_DEPS_MODE=default
USER 0
RUN mkdir -p /tmp/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/yum_temp/ || true
COPY .oit/unsigned.repo /etc/yum.repos.d/
ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp
# End Konflux-specific steps
RUN commands


# Start Konflux-specific steps
USER 0
RUN cp /tmp/yum_temp/* /etc/yum.repos.d/ || true
USER 3000
# End Konflux-specific steps
"""
        rebaser = KonfluxRebaser(MagicMock(), MagicMock(), MagicMock(), "unsigned")
        rebaser._add_build_repos(dfp=dfp, metadata=metadata, dest_dir=Path("."))

        self.assertEqual(dfp.content.strip(), expected.strip())
