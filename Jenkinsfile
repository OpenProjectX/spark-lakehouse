@Library('jenkins-pipelines@master') _

// Agent label must be passed here (not in ci.yaml): the YAML is only read
// after node allocation. Label matches the Kubernetes pod template whose jnlp
// container uses ghcr.io/openprojectx/jenkins-build-agent:latest.
ciPipeline(agent: 'gradle-long-running')
