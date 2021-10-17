all: requirement

requirement:
	pip install -r requirements.txt

run:
	python3 main.py
