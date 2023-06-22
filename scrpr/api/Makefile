TMP_DIR := /tmp/fastapi-backend-lxc
ARTIFACT := lxc-fastapi-backend.tar.gz
DOCKER := $(shell which docker)
# @@@ will not find
#PIP := $(shell which pip)

ensure_docker:
ifeq (,$(DOCKER))
	$(error requires docker)
endif
	@echo found docker: $(DOCKER)

test-pip: ensure_docker
	mkdir -p $(TMP_DIR)/app
	cp requirements.txt $(TMP_DIR)/app
	#$(PIP) install --target $(TMP_DIR)/app requirements.txt
	$(DOCKER) run --rm -v $(TMP_DIR)/app:/app:rw -it --entrypoint '/bin/sh' python:3 -c "/usr/local/bin/pip install --target=/app -r /app/requirements.txt"

build-lxc:
	mkdir $(TMP_DIR)
# @@@TODO
# perl: warning: Setting locale failed.
	#	LANGUAGE = (unset),
	#	LC_ALL = (unset),
	#	LANG = "en_US.UTF-8"
	#
	#	and also:
	#	locale: Cannot set LC_CTYPE to default locale: No such file or directory
	# locale: Cannot set LC_MESSAGES to default locale: No such file or directory
	# locale: Cannot set LC_ALL to default locale: No such file or directory


	sudo debootstrap sid $(TMP_DIR)
	sudo chroot $(TMP_DIR) /bin/bash -c 'apt install -y python3-pip'
	sudo mkdir $(TMP_DIR)/app
	sudo cp -r requirements.txt main.py sql_app/ $(TMP_DIR)
	docker run --rm -v $(TMP_DIR)/app:/app:rw -it --entrypoint '/bin/sh' python:3 -c "/usr/local/bin/pip install --target=/app -r /app/requirements.txt"

	sudo tar -czf ./$(ARTIFACT) -C $(TMP_DIR)/ .
	sudo chown $$USER:$$USER $(ARTIFACT)
	#sudo rm -rf $(TMP_DIR)

clean:
	sudo rm -rf $(TMP_DIR)
