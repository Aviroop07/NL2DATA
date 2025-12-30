/**
 * Validation utilities
 */

import { NL_INPUT_WORD_LIMIT } from './constants';
import { countWords } from './helpers';

/**
 * Validate NL description
 */
export function validateNLDescription(description: string): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];
  
  if (!description || description.trim().length === 0) {
    errors.push('Description cannot be empty');
  }
  
  if (description.trim().length < 10) {
    errors.push('Description must be at least 10 characters long');
  }
  
  const wordCount = countWords(description);
  if (wordCount > NL_INPUT_WORD_LIMIT) {
    errors.push(`Description exceeds word limit of ${NL_INPUT_WORD_LIMIT} words (current: ${wordCount})`);
  }
  
  return {
    valid: errors.length === 0,
    errors
  };
}

/**
 * Validate UUID format
 */
export function validateUUID(uuid: string): boolean {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  return uuidRegex.test(uuid);
}

