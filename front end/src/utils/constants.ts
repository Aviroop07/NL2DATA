/**
 * Word limit for NL input text area.
 * Configurable constant - change this value to adjust the word limit.
 */
export const NL_INPUT_WORD_LIMIT = 500;

/**
 * API base URL - defaults to localhost:8000
 */
export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

/**
 * WebSocket URL - defaults to ws://localhost:8000
 */
export const WS_BASE_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8000';

/**
 * Auto-save interval in milliseconds
 */
export const AUTO_SAVE_INTERVAL = 5000;

/**
 * Suggestions fetch interval in milliseconds
 */
export const SUGGESTIONS_INTERVAL = 5000;

/**
 * Maximum number of status trail items to keep
 */
export const MAX_STATUS_TRAIL_SIZE = 300;

/**
 * Maximum undo stack size
 */
export const MAX_UNDO_STACK_SIZE = 5;

