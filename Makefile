
CERTSUBJ := /C=JP/ST=Tokyo/O='sqlx-scylladb'
CERTSAN := IP:127.0.0.1,DNS:scylladb-tls,DNS:*.internal,DNS:*.local,DNS:localhost
CERTDIR := certs
CACERT := $(CERTDIR)/ca-cert.pem
CAKEY := $(CERTDIR)/ca-key.pem
CACSR := $(CERTDIR)/ca-csr.pem
SRVCERTKEY := $(CERTDIR)/server-key.pem
SRVCERTCSR := $(CERTDIR)/server-csr.pem
SRVCERT := $(CERTDIR)/server-cert.pem
CERTKEY := $(CERTDIR)/client-key.pem
CERTCSR := $(CERTDIR)/client-csr.pem
CERT := $(CERTDIR)/client-cert.pem

.PHONY: mkcert
mkcert:
	openssl genpkey -algorithm ec -pkeyopt ec_paramgen_curve:P-256 -out $(CAKEY) # CA秘密鍵の作成
	openssl req -new -key $(CAKEY) -out $(CACSR) -subj $(CERTSUBJ) # 証明書署名要求の作成
	openssl x509 -req -in $(CACSR) -signkey $(CAKEY) -days 3650 -out $(CACERT) # 認証局による公開鍵への自己署名

	openssl genpkey -algorithm ec -pkeyopt ec_paramgen_curve:P-256 -out $(SRVCERTKEY) # 秘密鍵の作成
	openssl req -new -key $(SRVCERTKEY) -out $(SRVCERTCSR) -subj $(CERTSUBJ) -addext 'subjectAltName = $(CERTSAN)' # 証明書署名要求の作成
	openssl x509 -req -in $(SRVCERTCSR) -CA $(CACERT) -CAkey $(CAKEY) -CAcreateserial -days 3650 -copy_extensions copy -out $(SRVCERT) # 認証局による証明書発行

	openssl genpkey -algorithm ec -pkeyopt ec_paramgen_curve:P-256 -out $(CERTKEY) # 秘密鍵の作成
	openssl req -new -key $(CERTKEY) -out $(CERTCSR) -subj $(CERTSUBJ) -addext 'subjectAltName = $(CERTSAN)' # 証明書署名要求の作成
	openssl x509 -req -in $(CERTCSR) -CA $(CACERT) -CAkey $(CAKEY) -CAcreateserial -days 3650 -copy_extensions copy -out $(CERT) # 認証局による証明書発行
