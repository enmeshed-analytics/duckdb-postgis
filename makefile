DATE := $(shell date +%Y-%m-%d)

define COMMIT_TYPES
feat:     A new feature
fix:      A bug fix
docs:     Documentation only changes
style:    Changes that do not affect the meaning of the code
refactor: A code change that neither fixes a bug nor adds a feature
perf:     A code change that improves performance
test:     Adding missing tests or correcting existing tests
build:    Changes that affect the build system or external dependencies
ci:       Changes to CI configuration files and scripts
chore:    Other changes that don't modify src or test files
revert:   Reverts a previous commit
endef
export COMMIT_TYPES

repo-update: git-add git-commit git-push

git-add:
	git add .

git-commit:
	@echo "Available commit types:"
	@echo "$$COMMIT_TYPES" | sed 's/^/  /'
	@echo
	@read -p "Enter commit type: " type; \
	if echo "$$COMMIT_TYPES" | grep -q "^$$type:"; then \
		read -p "Enter commit scope (optional, press enter to skip): " scope; \
		read -p "Is this a breaking change? (y/N): " breaking; \
		read -p "Enter commit message: " msg; \
		if [ "$$breaking" = "y" ] || [ "$$breaking" = "Y" ]; then \
			if [ -n "$$scope" ]; then \
				git commit -m "$$type!($$scope): $$msg [$(DATE)]" -m "BREAKING CHANGE: $$msg"; \
			else \
				git commit -m "$$type!: $$msg [$(DATE)]" -m "BREAKING CHANGE: $$msg"; \
			fi; \
		else \
			if [ -n "$$scope" ]; then \
				git commit -m "$$type($$scope): $$msg [$(DATE)]"; \
			else \
				git commit -m "$$type: $$msg [$(DATE)]"; \
			fi; \
		fi; \
	else \
		echo "Invalid commit type. Please use one of the available types."; \
		exit 1; \
	fi

git-push:
	git push
