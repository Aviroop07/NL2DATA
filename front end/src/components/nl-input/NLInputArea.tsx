import React, { useState, useEffect, useCallback } from 'react';
import { TextField, Box, Typography, Button, Paper } from '@mui/material';
import { useAppStore } from '../../stores/useAppStore';
import { useAutoSave } from '../../hooks/useAutoSave';
import { useSuggestions } from '../../hooks/useSuggestions';
import { NL_INPUT_WORD_LIMIT } from '../../utils/constants';
import { countWords } from '../../utils/helpers';
import { validateNLDescription } from '../../utils/validation';
import QualityBar from './QualityBar';

const NLInputArea: React.FC = () => {
  const { nlDescription, setNLDescription, submitProcessing, processing } = useAppStore();
  const [wordCount, setWordCount] = useState(0);
  const [charCount, setCharCount] = useState(0);
  
  // Auto-save every 5 seconds (only when not processing)
  useAutoSave(nlDescription, 5000);
  
  // Fetch suggestions every 5 seconds (only when not processing)
  useSuggestions(nlDescription, 5000);
  
  // After processing starts, show read-only view
  const isReadOnly = processing;
  
  // Calculate word count
  useEffect(() => {
    const words = nlDescription.trim().split(/\s+/).filter(Boolean);
    setWordCount(words.length);
    setCharCount(nlDescription.length);
  }, [nlDescription]);
  
  const handleChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    const value = event.target.value;
    
    // Calculate current word count
    const words = value.trim().split(/\s+/).filter(Boolean);
    const currentWordCount = words.length;
    
    // Prevent input if word limit exceeded
    if (currentWordCount > NL_INPUT_WORD_LIMIT) {
      // Find the last valid position (before the word that would exceed limit)
      const wordsBeforeLimit = words.slice(0, NL_INPUT_WORD_LIMIT);
      const lastValidIndex = value.lastIndexOf(wordsBeforeLimit[wordsBeforeLimit.length - 1] || '');
      const truncatedValue = value.substring(0, lastValidIndex + (wordsBeforeLimit[wordsBeforeLimit.length - 1]?.length || 0));
      setNLDescription(truncatedValue);
      return;
    }
    
    setNLDescription(value);
  };
  
  const handleSubmit = useCallback(async () => {
    console.log('Submit button clicked');
    console.log('Current NL description length:', nlDescription.length);
    
    const validation = validateNLDescription(nlDescription);
    if (!validation.valid) {
      console.log('Validation failed:', validation.errors);
      alert(validation.errors.join('\n'));
      return;
    }
    
    console.log('Validation passed, calling submitProcessing...');
    try {
      await submitProcessing(nlDescription);
      console.log('submitProcessing completed');
    } catch (error) {
      console.error('Error in submitProcessing:', error);
    }
  }, [nlDescription, submitProcessing]);
  
  const isWordLimitExceeded = wordCount > NL_INPUT_WORD_LIMIT;
  const wordCountColor = isWordLimitExceeded ? 'error' : wordCount > NL_INPUT_WORD_LIMIT * 0.9 ? 'warning.main' : 'text.secondary';
  
  return (
    <Box sx={{ p: 4 }}>
      <Box sx={{ mb: 3 }}>
        <Typography variant="h5" gutterBottom sx={{ fontWeight: 600, color: 'text.primary' }}>
          Natural Language Description
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Describe your database requirements in natural language. Our AI will help you refine and improve your description.
        </Typography>
      </Box>
      
      {isReadOnly ? (
        // Read-only view after submission
        <Paper
          elevation={0}
          sx={{
            p: 3,
            mb: 3,
            border: '1px solid',
            borderColor: 'divider',
            borderRadius: 2,
            bgcolor: 'grey.50',
          }}
        >
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
            <Typography variant="body2" color="text.secondary" sx={{ fontWeight: 600 }}>
              Submitted Description
            </Typography>
            <Typography variant="caption" color="text.secondary">
              {wordCount} words • {charCount} characters
            </Typography>
          </Box>
          <Typography
            variant="body1"
            sx={{
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              lineHeight: 1.8,
              color: 'text.primary',
            }}
          >
            {nlDescription || '(No description provided)'}
          </Typography>
        </Paper>
      ) : (
        // Editable input view
        <Box sx={{ position: 'relative', mb: 3 }}>
          <TextField
            fullWidth
            multiline
            minRows={10}
            maxRows={15}
            value={nlDescription}
            onChange={handleChange}
            placeholder="Describe your database requirements here. For example: 'I need a database for an e-commerce store with customers, products, and orders. Customers can place multiple orders, and each order contains multiple products...'"
            error={isWordLimitExceeded}
            helperText={isWordLimitExceeded ? `Word limit exceeded (${NL_INPUT_WORD_LIMIT} words maximum)` : 'Start typing to get intelligent suggestions'}
            sx={{
              '& .MuiOutlinedInput-root': {
                fontSize: '0.95rem',
                lineHeight: 1.6,
              },
            }}
          />
          
          {/* Word count at bottom-right corner of text box */}
          <Box
            sx={{
              position: 'absolute',
              bottom: 24,
              right: 16,
              bgcolor: isWordLimitExceeded ? 'error.light' : 'background.paper',
              px: 1.5,
              py: 0.5,
              borderRadius: 2,
              border: '1px solid',
              borderColor: isWordLimitExceeded ? 'error.main' : 'divider',
              boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
            }}
          >
            <Typography
              variant="caption"
              sx={{
                color: isWordLimitExceeded ? 'error.contrastText' : wordCountColor,
                fontWeight: isWordLimitExceeded ? 700 : 600,
                fontSize: '0.75rem',
              }}
            >
              {wordCount} / {NL_INPUT_WORD_LIMIT} words
            </Typography>
          </Box>
        </Box>
      )}
      
      {!isReadOnly && <QualityBar />}
      
      {!isReadOnly && (
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 3 }}>
          <Typography variant="body2" color="text.secondary">
            {charCount} characters
          </Typography>
          <Button
            variant="contained"
            size="large"
            onClick={handleSubmit}
            disabled={processing || nlDescription.trim().length < 10 || isWordLimitExceeded}
            sx={{
              minWidth: 120,
              fontWeight: 600,
            }}
          >
            {processing ? 'Processing...' : 'Submit'}
          </Button>
        </Box>
      )}
    </Box>
  );
};

export default NLInputArea;

