#!/bin/bash

echo "Встановлення залежностей для addrinity-migration..."

# Оновлення pip
pip install --upgrade pip

# Встановлення залежностей
pip install -r requirements.txt

echo "Залежності встановлено успішно!"