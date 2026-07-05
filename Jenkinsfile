@Library('jenkins-pipelines@master') _

// Agent label must be passed here (not in ci.yaml): the YAML is only read
// after node allocation. Label matches the Kubernetes pod template in k8s-infra.
ciPipeline(agent: 'gradle-long-running')
