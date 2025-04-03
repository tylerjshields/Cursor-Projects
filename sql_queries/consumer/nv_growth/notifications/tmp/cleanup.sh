#!/bin/bash
# Cleanup script for temporary SQL files

# Directory where temporary files are stored
TMP_DIR="sql_queries/consumer/nv_growth/notifications/tmp"

# List of files we'll keep for reference
KEEP_FILES=(
  "fix_missing_campaigns.sql"       # Main fix for the EP name issue
  "update_full_pipeline.sql"        # Full pipeline rebuild script
  "cleanup.sh"                     # This script
)

echo "Cleaning up temporary files in $TMP_DIR"
echo "Keeping important files: ${KEEP_FILES[@]}"

# Count of deleted files
deleted=0

# Loop through all SQL files in the directory
for file in "$TMP_DIR"/*.sql; do
  filename=$(basename "$file")
  
  # Check if file should be kept
  keep=false
  for keep_file in "${KEEP_FILES[@]}"; do
    if [ "$filename" == "$keep_file" ]; then
      keep=true
      break
    fi
  done
  
  # Delete file if not in keep list
  if [ "$keep" == false ]; then
    echo "Deleting: $filename"
    rm "$file"
    deleted=$((deleted + 1))
  else
    echo "Keeping: $filename"
  fi
done

echo "--------------------------------------"
echo "Cleanup complete. Deleted $deleted files."
echo "Kept ${#KEEP_FILES[@]} important files." 