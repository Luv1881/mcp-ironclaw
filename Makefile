GO ?= go
CLANG ?= clang
BPF_SOURCE := bpf/ironclaw.c
BPF_OBJECT := internal/ebpf/ironclaw.bpf.o
BPF_CFLAGS := -O2 -g -target bpf -D__TARGET_ARCH_x86 -Wall -Werror

COMPOSE := deploy/docker-compose.yml
DOCKER ?= docker
PKI_DIR ?= deploy/pki/out

.PHONY: all build test race cover vet fmt fmt-check bpf bpf-build lint clean run \
        infra-up infra-down infra-logs integration pki pki-force haproxy-check mtls-test \
        edge-image edge-up edge-down edge-test keystore loadtest \
        images k8s-validate k8s-up k8s-down k8s-status tf-validate ironclaw-verify \
        freshness chaos-up chaos-down chaos-broker-kill lint agent

all: fmt-check vet lint test

lint: $(BPF_OBJECT)
	golangci-lint run ./...

agent:
	$(GO) build -o bin/ironclaw-agent ./agent

build:
	$(GO) build ./...

test:
	$(GO) test -count=1 ./...

race:
	$(GO) test -race -count=1 ./...

cover:
	$(GO) test -count=1 -cover ./internal/...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

fmt-check:
	@test -z "$$(gofmt -l . | grep -v '^vendor/')" || (gofmt -l . && echo "run make fmt" && exit 1)

$(BPF_OBJECT): $(BPF_SOURCE)
	$(CLANG) $(BPF_CFLAGS) -c $(BPF_SOURCE) -o $(BPF_OBJECT)
	llvm-strip -g $(BPF_OBJECT)

bpf: $(BPF_OBJECT)

bpf-build: bpf
	$(GO) build -tags ebpf ./...

infra-up:
	$(DOCKER) compose -f $(COMPOSE) up -d --wait

infra-down:
	$(DOCKER) compose -f $(COMPOSE) down -v --remove-orphans

infra-logs:
	$(DOCKER) compose -f $(COMPOSE) logs --tail=50

integration: infra-up
	$(GO) test -race -count=1 ./internal/redisstore/... ./internal/postgresstore/... ./internal/kafkabus/...

pki:
	DEVICE_COUNT=$(or $(DEVICE_COUNT),20) ./deploy/pki/issue-certs.sh $(PKI_DIR)

pki-force:
	rm -rf $(PKI_DIR)
	$(MAKE) pki

haproxy-check: pki
	$(DOCKER) run --rm \
	    -v $(CURDIR)/deploy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro \
	    -v $(CURDIR)/$(PKI_DIR):/etc/ironclaw/tls:ro \
	    haproxy:3.0-alpine haproxy -c -f /usr/local/etc/haproxy/haproxy.cfg

mtls-test: pki
	PKI_DIR=$(CURDIR)/$(PKI_DIR) DOCKER="$(DOCKER)" ./deploy/verify-mtls.sh

ironclaw-verify: pki
	mkdir -p bin
	$(GO) build -o bin/mcp-server ./mcp-server
	./deploy/verify-ironclaw-integration.sh

edge-image:
	mkdir -p deploy/build
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o deploy/build/ingest-linux-amd64 ./ingest
	$(DOCKER) build -f deploy/Dockerfile.ingest -t ironclaw/ingest:dev deploy/

edge-up: pki edge-image
	$(DOCKER) rm -f ironclaw-lt-ingest ironclaw-lt-edge 2>/dev/null || true
	$(DOCKER) run -d --name ironclaw-lt-ingest --network ironclaw_default --user "$$(id -u):$$(id -g)" \
	    --network-alias ingest-a.ironclaw.internal --network-alias ingest.ironclaw.internal \
	    -v $(CURDIR)/$(PKI_DIR):/etc/ironclaw/tls:ro ironclaw/ingest:dev \
	    -addr :8443 -cert /etc/ironclaw/tls/ingest.crt -key /etc/ironclaw/tls/ingest.key \
	    -client-ca /etc/ironclaw/tls/ca.crt -kafka kafka:9092 -topic ironclaw.events.raw -redis redis:6379
	$(DOCKER) run -d --name ironclaw-lt-edge --user root --network ironclaw_default \
	    --network-alias edge.ironclaw.internal -p 9443:443 \
	    -v $(CURDIR)/deploy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro \
	    -v $(CURDIR)/$(PKI_DIR):/etc/ironclaw/tls:ro haproxy:3.0-alpine

edge-down:
	$(DOCKER) rm -f ironclaw-lt-ingest ironclaw-lt-edge 2>/dev/null || true

edge-test: pki edge-image
	PKI_DIR=$(CURDIR)/$(PKI_DIR) DOCKER="$(DOCKER)" ./deploy/verify-edge-path.sh

keystore: pki
	PKI_DIR=$(CURDIR)/$(PKI_DIR) DOCKER="$(DOCKER)" ./loadtest/build-keystore.sh

loadtest: keystore
	mkdir -p loadtest/out/results
	$(DOCKER) run --rm --network ironclaw_default --user "$$(id -u):$$(id -g)" \
	    -v $(CURDIR)/loadtest:/loadtest -w /loadtest/out justb4/jmeter:5.5 \
	    -n -t /loadtest/ironclaw-plan.jmx \
	    -Jedge.host=edge.ironclaw.internal -Jedge.port=443 \
	    -Jdevices=$(or $(DEVICES),20) -Jramp=5 -Jduration=$(or $(DURATION),60) -JeventsPerBatch=100 \
	    -Jhttps.default.protocol=TLSv1.3 -Djdk.tls.client.protocols=TLSv1.3 \
	    -Djavax.net.ssl.keyStore=/loadtest/out/ironclaw.p12 -Djavax.net.ssl.keyStorePassword=ironclaw \
	    -Djavax.net.ssl.keyStoreType=PKCS12 \
	    -Djavax.net.ssl.trustStore=/loadtest/out/truststore.jks -Djavax.net.ssl.trustStorePassword=ironclaw \
	    -Dhttps.use.cachedSSLContext=false \
	    -l /loadtest/out/results/run.jtl

KIND_CLUSTER ?= ironclaw
K8S_VERSION ?= 1.31.0
CRD_SCHEMAS := https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json

images:
	mkdir -p deploy/build
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o deploy/build/ingest-linux-amd64 ./ingest
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o deploy/build/mcp-server-linux-amd64 ./mcp-server
	$(DOCKER) build -f deploy/Dockerfile.ingest -t ironclaw/ingest:dev deploy/
	$(DOCKER) build -f deploy/Dockerfile.mcpserver -t ironclaw/mcp-server:dev deploy/

k8s-validate:
	kubeconform -strict -summary -kubernetes-version $(K8S_VERSION) \
	    -ignore-filename-pattern kustomization.yaml deploy/k8s/base/ deploy/k8s/dev/
	kubeconform -strict -summary -kubernetes-version $(K8S_VERSION) \
	    -ignore-filename-pattern kustomization.yaml \
	    -schema-location default -schema-location '$(CRD_SCHEMAS)' deploy/k8s/addons/
	kubectl kustomize deploy/k8s/dev | kubeconform -strict -summary -kubernetes-version $(K8S_VERSION)

k8s-up: pki images
	kind get clusters 2>/dev/null | grep -qx $(KIND_CLUSTER) || kind create cluster --name $(KIND_CLUSTER) --wait 120s
	kind load docker-image --name $(KIND_CLUSTER) ironclaw/ingest:dev ironclaw/mcp-server:dev
	kubectl apply -f deploy/k8s/base/00-namespace.yaml
	kubectl -n ironclaw create secret generic ironclaw-ingest-tls \
	    --from-file=tls.crt=$(PKI_DIR)/ingest.crt --from-file=tls.key=$(PKI_DIR)/ingest.key \
	    --from-file=ca.crt=$(PKI_DIR)/ca.crt --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n ironclaw create secret generic ironclaw-edge-tls \
	    --from-file=edge.pem=$(PKI_DIR)/edge.pem --from-file=ca.crt=$(PKI_DIR)/ca.crt \
	    --from-file=devices.crl=$(PKI_DIR)/devices.crl --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n ironclaw create secret generic ironclaw-postgres \
	    --from-literal=dsn='postgres://ironclaw:ironclaw@postgres:5432/ironclaw?sslmode=disable' \
	    --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -k deploy/k8s/dev
	kubectl -n ironclaw wait --for=condition=Available --timeout=300s deployment --all

k8s-status:
	kubectl -n ironclaw get pods,svc,hpa,pdb

k8s-down:
	kind delete cluster --name $(KIND_CLUSTER)

tf-validate:
	cd deploy/terraform && terraform init -input=false -backend=false >/dev/null && terraform fmt -check -recursive && terraform validate

freshness:
	$(GO) run ./loadtest/freshness \
	    -endpoint "https://localhost:9443/v1/batches" -server-name edge.ironclaw.internal \
	    -cert $(PKI_DIR)/device-000.crt -key $(PKI_DIR)/device-000.key -ca $(PKI_DIR)/ca.crt \
	    -redis localhost:16379 -rounds $(or $(ROUNDS),40) -budget $(or $(BUDGET),5s)

chaos-up:
	$(DOCKER) compose -f deploy/docker-compose.ha.yml up -d --wait

chaos-down:
	$(DOCKER) compose -f deploy/docker-compose.ha.yml down -v --remove-orphans
	$(DOCKER) rm -f ironclaw-ha-ingest ironclaw-ha-edge 2>/dev/null || true

chaos-broker-kill:
	$(DOCKER) kill ironclaw-ha-kafka2
	@echo "broker 2 killed; compare ingest_events_accepted with events_counted once the load drains"

run:
	$(GO) run ./mcp-server

clean:
	rm -f $(BPF_OBJECT)
	$(GO) clean -cache -testcache
