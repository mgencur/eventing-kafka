#!/usr/bin/env bash

export EVENTING_NAMESPACE="${EVENTING_NAMESPACE:-knative-eventing}"
export SYSTEM_NAMESPACE=$EVENTING_NAMESPACE
export KNATIVE_DEFAULT_NAMESPACE=$EVENTING_NAMESPACE
export ZIPKIN_NAMESPACE=$EVENTING_NAMESPACE
export CONFIG_TRACING_CONFIG="test/config/config-tracing.yaml"
export STRIMZI_INSTALLATION_CONFIG_TEMPLATE="test/config/100-strimzi-cluster-operator-0.27.0.yaml"
export STRIMZI_INSTALLATION_CONFIG="$(mktemp)"
export KAFKA_INSTALLATION_CONFIG="test/config/100-kafka-ephemeral-triple-3.0.0.yaml"
export KAFKA_USERS_CONFIG="test/config/100-strimzi-users.yaml"
export KAFKA_PLAIN_CLUSTER_URL="my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"
readonly KNATIVE_EVENTING_MONITORING_YAML="test/config/monitoring.yaml"
readonly SO_REPO="https://github.com/openshift-knative/serverless-operator.git"
readonly EVENTING_REPO="https://github.com/openshift/knative-eventing.git"
readonly SO_OLM_PROJECT="https://raw.githubusercontent.com/openshift-knative/serverless-operator/main/olm-catalog/serverless-operator/project.yaml"
readonly CURRENT_MODULE="eventing_kafka"
KAFKA_CLUSTER_URL=${KAFKA_PLAIN_CLUSTER_URL}
export EVENTING_KAFKA_TEST_IMAGE_TEMPLATE=$(cat <<-END
{{- with .Name }}
{{- if eq . "event-sender"}}$KNATIVE_EVENTING_KAFKA_TEST_EVENT_SENDER{{end -}}
{{- if eq . "heartbeats"}}$KNATIVE_EVENTING_KAFKA_TEST_HEARTBEATS{{end -}}
{{- if eq . "kafka-publisher"}}$KNATIVE_EVENTING_KAFKA_TEST_KAFKA_PUBLISHER{{end -}}
{{- if eq . "kafka_performance"}}$KNATIVE_EVENTING_KAFKA_TEST_KAFKA_PERFORMANCE{{end -}}
{{- if eq . "performance"}}$KNATIVE_EVENTING_KAFKA_TEST_PERFORMANCE{{end -}}
{{- if eq . "print"}}$KNATIVE_EVENTING_KAFKA_TEST_PRINT{{end -}}
{{- if eq . "recordevents"}}$KNATIVE_EVENTING_KAFKA_TEST_RECORDEVENTS{{end -}}
{{- if eq . "wathola-fetcher"}}$KNATIVE_EVENTING_KAFKA_TEST_WATHOLA_FETCHER{{end -}}
{{- if eq . "wathola-forwarder"}}$KNATIVE_EVENTING_KAFKA_TEST_WATHOLA_FORWARDER{{end -}}
{{- if eq . "wathola-kafka-sender"}}$KNATIVE_EVENTING_KAFKA_TEST_WATHOLA_KAFKA_SENDER{{end -}}
{{- if eq . "wathola-receiver"}}$KNATIVE_EVENTING_KAFKA_TEST_WATHOLA_RECEIVER{{end -}}
{{- if eq . "wathola-sender"}}$KNATIVE_EVENTING_KAFKA_TEST_WATHOLA_SENDER{{end -}}
{{end -}}
END
)

function scale_up_workers(){
  local cluster_api_ns="openshift-machine-api"

  oc get machineset -n ${cluster_api_ns} --show-labels

  # Get the name of the first machineset that has at least 1 replica
  local machineset
  machineset=$(oc get machineset -n ${cluster_api_ns} -o custom-columns="name:{.metadata.name},replicas:{.spec.replicas}" | grep " 1" | head -n 1 | awk '{print $1}')
  # Bump the number of replicas to 6 (+ 1 + 1 == 8 workers)
  oc patch machineset -n ${cluster_api_ns} "${machineset}" -p '{"spec":{"replicas":6}}' --type=merge
  wait_until_machineset_scales_up ${cluster_api_ns} "${machineset}" 6
}

# Waits until the machineset in the given namespaces scales up to the
# desired number of replicas
# Parameters: $1 - namespace
#             $2 - machineset name
#             $3 - desired number of replicas
function wait_until_machineset_scales_up() {
  echo -n "Waiting until machineset $2 in namespace $1 scales up to $3 replicas"
  for _ in {1..150}; do  # timeout after 15 minutes
    local available
    available=$(oc get machineset -n "$1" "$2" -o jsonpath="{.status.availableReplicas}")
    if [[ ${available} -eq $3 ]]; then
      echo -e "\nMachineSet $2 in namespace $1 successfully scaled up to $3 replicas"
      return 0
    fi
    echo -n "."
    sleep 6
  done
  echo - "Error: timeout waiting for machineset $2 in namespace $1 to scale up to $3 replicas"
  return 1
}

# Loops until duration (car) is exceeded or command (cdr) returns non-zero
function timeout() {
  SECONDS=0; TIMEOUT=$1; shift
  while eval $*; do
    sleep 5
    [[ $SECONDS -gt $TIMEOUT ]] && echo "ERROR: Timed out" && return 1
  done
  return 0
}

# Setup zipkin
function install_tracing() {
  echo "Installing Zipkin..."
  sed "s/\${SYSTEM_NAMESPACE}/${SYSTEM_NAMESPACE}/g" < "${KNATIVE_EVENTING_MONITORING_YAML}" | oc apply -f -
  wait_until_pods_running "${SYSTEM_NAMESPACE}" || fail_test "Zipkin inside eventing did not come up"
  oc -n "${SYSTEM_NAMESPACE}" patch knativeeventing/knative-eventing --type=merge --patch='{"spec": {"config": { "tracing": {"enable":"true","backend":"zipkin", "zipkin-endpoint":"http://zipkin.'${SYSTEM_NAMESPACE}'.svc.cluster.local:9411/api/v2/spans", "debug":"true", "sample-rate":"1.0"}}}}'
}

function install_strimzi(){
  header "Installing Kafka cluster"
  oc create namespace kafka || return 1
  sed 's/namespace: .*/namespace: kafka/' ${STRIMZI_INSTALLATION_CONFIG_TEMPLATE} > ${STRIMZI_INSTALLATION_CONFIG}
  oc apply -f "${STRIMZI_INSTALLATION_CONFIG}" -n kafka || return 1
  # Wait for the CRD we need to actually be active
  oc wait crd --timeout=900s kafkas.kafka.strimzi.io --for=condition=Established || return 1

  oc apply -f ${KAFKA_INSTALLATION_CONFIG} -n kafka
  oc wait kafka --all --timeout=900s --for=condition=Ready -n kafka || return 1

  # Create some Strimzi Kafka Users
  oc apply -f "${KAFKA_USERS_CONFIG}" -n kafka || return 1
}

################################################################################
# Simple function to do arithmetic calculation using AWK
#
# Globals:
#   None
# Arguments:
#   *: Arithmetic expression to calculate (e.g. 3 + 5 , 8/2)
# Output:
#   Writes the calculated value to stdout
################################################################################
function calc() {
    awk "BEGIN { print $*; }"
}

################################################################################
# Install eventing from midstream repo the matches the current branch
#
# The current branch name should match the midstream eventing branch used.
# Globals:
#   EVENTING_REPO
#   EVENTING_NAMESPACE
# Arguments:
#   None
################################################################################
function install_midstream_eventing() {
  local branch=$(git rev-parse --abbrev-ref HEAD)
  header "Installing Knative Eventing from ${branch}"
  local eventing_dir=/tmp/eventing-operator
  local failed=0
  git clone --branch $branch $EVENTING_REPO $eventing_dir || return 1
  pushd $eventing_dir || return 1

  cat openshift/release/knative-eventing-ci.yaml > ci
  cat openshift/release/knative-eventing-mtbroker-ci.yaml >> ci

  oc apply -f ci || return 1
  rm ci

  # Wait for 5 pods to appear first
  timeout 900 '[[ $(oc get pods -n $EVENTING_NAMESPACE --no-headers | wc -l) -lt 5 ]]' || return 1
  wait_until_pods_running $EVENTING_NAMESPACE || return 1

  popd || return 1
  return $failed
}

################################################################################
# Calculate the serverless operator release which corresponds to the given
# eventing version regardless of whether that release exists or not.
#
# e.g eventing 0.25 -> 1.19, eventing 1.0 -> 1.21, eventing 1.2 -> 1.23.
# Globals:
#   None
# Arguments:
#   1: Eventing version number. E.g. 0.26 or 1.2
# Outputs:
#   Writes the calculated release version to stdout
################################################################################
function calculate_so_release() {
    version=$1
    if [[ "$version" < 1 ]];then
        printf "%.2f\n" "$(calc $version + 0.94)"
    elif [[ "$version" == "1.0" ]];then
        echo 1.21
    else
       printf "%.2f\n" "$(calc "$version" / 10 + "1.11")"
    fi
}

################################################################################
# Find the serverless operator branch which provides the eventing version that
# matches the current branch version whether it's a release branch or main.
#
# Globals:
#   SO_REPO
# Arguments:
#   None
# Outputs:
#   Writes the branch name to stdout or 0 if none was found.
################################################################################
function find_matching_so_release_branch(){
  local current_branch=$(git rev-parse --abbrev-ref HEAD)
  if [[ $current_branch == release-v* ]]; then
    version=$(echo "$current_branch"|cut -dv -f2)
    so_version=$(calculate_so_release "$version")
    if git ls-remote -h $SO_REPO | grep -F "release-$so_version" ; then
      echo "${so_version}"
      return 1
    else
      # There's no matching SO version. Let's see if main is a good candidate.
      # We will search SO main branch's olm-catalog/serverless-operator/project.yaml file
      if curl -s $SO_OLM_PROJECT | grep -q "${CURRENT_MODULE}:\s*${version}" ; then
        echo "main"
        return 1
      fi
    fi
  fi
  echo 0
}

################################################################################
# Install serverless operator from given branch and optionally skip eventing
# installation.
# Globals:
#   SO_REPO
# Arguments:
#   1: Branch name to install from
#   2: Skip installing eventing from serverless operator. Default is false.
################################################################################
function install_serverless_operator_custom() {
  local so_branch=$1
  local skip_eventing=${2:-false}
  local so_install_cmd="./hack/install.sh"
  if [[ "$skip_eventing" == "true" ]]; then
    so_install_cmd='INSTALL_EVENTING="false" ./hack/install.sh'
  fi
  header "Installing Serverless Operator from ${so_branch}"
  local operator_dir=/tmp/serverless-operator
  local failed=0
  git clone --branch $so_branch $SO_REPO $operator_dir || return 1
  # unset OPENSHIFT_BUILD_NAMESPACE (old CI) and OPENSHIFT_CI (new CI) as its used in serverless-operator's CI
  # environment as a switch to use CI built images, we want pre-built images of k-s-o and k-o-i
  unset OPENSHIFT_BUILD_NAMESPACE
  unset OPENSHIFT_CI
  pushd $operator_dir

  eval $so_install_cmd && header "Serverless Operator installed successfully" || failed=1
  popd
  return $failed
}

################################################################################
# Install serverless operator with the suitable eventing version
#
# By default, this function will check if there's a matching eventing version provided by
# serverless operator and if not (e.g release-next is always ahead of serverless
# operator) it will install the matching eventing version from midstream
# eventing. You can enforce using Serverless Operator eventing by passing false.
# Globals:
#   None
# Arguments:
#   1: Use midstream eventing if serverless operator doesn't provide needed
#      version. (default is true).
################################################################################
function install_serverless(){
  local fallback_to_midstream=${1:-true}
  local so_branch="main"
  local so_skip_eventing="false"
  if [[ "$fallback_to_midstream" == "true" ]]; then
    # Check if the same version of eventing is available via the serverless operator
    local found_branch=$(find_matching_so_release_branch)
    if [[ "$found_branch" == "0" ]]; then
      install_midstream_eventing || return 1
      found_branch="main"
      so_skip_eventing="true"
    fi
    so_branch="$found_branch"
  fi

  install_serverless_operator_custom "$so_branch" "$so_skip_eventing" || return 1
}

function install_knative_kafka {
  install_consolidated_knative_kafka_channel || return 1
  install_knative_kafka_source || return 1
}

function install_consolidated_knative_kafka_channel(){
  header "Installing Knative Kafka Channel"

  RELEASE_YAML="openshift/release/knative-eventing-kafka-channel-ci.yaml"

  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-consolidated-controller|${KNATIVE_EVENTING_KAFKA_CONSOLIDATED_CONTROLLER}|g" ${RELEASE_YAML}
  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-consolidated-dispatcher|${KNATIVE_EVENTING_KAFKA_CONSOLIDATED_DISPATCHER}|g" ${RELEASE_YAML}
  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-webhook|${KNATIVE_EVENTING_KAFKA_WEBHOOK}|g"                                 ${RELEASE_YAML}

  cat ${RELEASE_YAML} \
  | sed "s/REPLACE_WITH_CLUSTER_URL/${KAFKA_CLUSTER_URL}/" \
  | oc apply --filename -

  wait_until_pods_running $EVENTING_NAMESPACE || return 1
}

function install_knative_kafka_source(){
  header "Installing Knative Kafka Source"

  RELEASE_YAML="openshift/release/knative-eventing-kafka-source-ci.yaml"

  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-source-controller|${KNATIVE_EVENTING_KAFKA_SOURCE_CONTROLLER}|g"   ${RELEASE_YAML}
  sed -i -e "s|registry.ci.openshift.org/openshift/knative-.*:knative-eventing-kafka-receive-adapter|${KNATIVE_EVENTING_KAFKA_RECEIVE_ADAPTER}|g"       ${RELEASE_YAML}

  cat ${RELEASE_YAML} \
  | oc apply --filename -

  wait_until_pods_running $EVENTING_NAMESPACE || return 1
}

function create_auth_secrets() {
  create_tls_secrets
  create_sasl_secrets
}

function create_tls_secrets() {
  header "Creating TLS Kafka secret"
  STRIMZI_CRT=$(oc -n kafka get secret my-cluster-cluster-ca-cert --template='{{index .data "ca.crt"}}' | base64 --decode )
  TLSUSER_CRT=$(oc -n kafka get secret my-tls-user --template='{{index .data "user.crt"}}' | base64 --decode )
  TLSUSER_KEY=$(oc -n kafka get secret my-tls-user --template='{{index .data "user.key"}}' | base64 --decode )

  sleep 10

  oc create secret --namespace knative-eventing generic strimzi-tls-secret \
    --from-literal=ca.crt="$STRIMZI_CRT" \
    --from-literal=user.crt="$TLSUSER_CRT" \
    --from-literal=user.key="$TLSUSER_KEY" || return 1
}

function create_sasl_secrets() {
  header "Creating SASL Kafka secret"
  STRIMZI_CRT=$(oc -n kafka get secret my-cluster-cluster-ca-cert --template='{{index .data "ca.crt"}}' | base64 --decode )
  SASL_PASSWD=$(oc -n kafka get secret my-sasl-user --template='{{index .data "password"}}' | base64 --decode )

  sleep 10

  oc create secret --namespace knative-eventing generic strimzi-sasl-secret \
    --from-literal=ca.crt="$STRIMZI_CRT" \
    --from-literal=password="$SASL_PASSWD" \
    --from-literal=saslType="SCRAM-SHA-512" \
    --from-literal=user="my-sasl-user" || return 1
}

function run_e2e_tests(){
  header "Testing the KafkaChannel with no AUTH"

  # the source tests REQUIRE the secrets, hence we create it here:
  create_auth_secrets || return 1

  local test_name="${1:-}"
  local run_command=""
  local failed=0
  local channels=messaging.knative.dev/v1beta1:KafkaChannel

  local common_opts=" -channels=$channels --kubeconfig $KUBECONFIG"
  if [ -n "$test_name" ]; then
      local run_command="-run ^(${test_name})$"
  fi

  go_test_e2e -tags=e2e,consolidated -timeout=90m -parallel=12 ./test/e2e \
    "$run_command" \
    --imagetemplate "${TEST_IMAGE_TEMPLATE}" \
    $common_opts || failed=$?

  return $failed
}
