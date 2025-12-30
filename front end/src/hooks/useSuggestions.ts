import { useEffect, useRef } from 'react';
import { useAppStore } from '../stores/useAppStore';
import { apiService } from '../services/apiService';
import { SUGGESTIONS_INTERVAL } from '../utils/constants';

export const useSuggestions = (nlDescription: string, interval: number = SUGGESTIONS_INTERVAL) => {
  const { setKeywordSuggestions, setExtractedItems, processing } = useAppStore();
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const previousDescriptionRef = useRef<string>('');
  
  useEffect(() => {
    // Stop suggestions if processing has started
    if (processing) {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
      setKeywordSuggestions([]);
      setExtractedItems(null);
      previousDescriptionRef.current = nlDescription; // Reset to current to avoid false change detection
      return;
    }
    
    // Clear any existing interval
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    
    // Don't fetch if description is too short
    if (!nlDescription.trim() || nlDescription.trim().length < 10) {
      setKeywordSuggestions([]);
      setExtractedItems(null);
      previousDescriptionRef.current = nlDescription;
      return;
    }
    
    // Set up interval to check every 5 seconds
    intervalRef.current = setInterval(() => {
      // Don't check if processing has started
      const currentProcessing = useAppStore.getState().processing;
      if (currentProcessing) {
        if (intervalRef.current) {
          clearInterval(intervalRef.current);
          intervalRef.current = null;
        }
        return;
      }
      
      const currentDesc = nlDescription;
      
      // Check if description has changed since last check
      if (currentDesc !== previousDescriptionRef.current) {
        // Description changed - send request to backend
        console.log('NL description changed, fetching suggestions...');
        apiService.getSuggestions({ nl_description: currentDesc })
          .then((response) => {
            setKeywordSuggestions(response.keywords);
            setExtractedItems(response.extracted_items);
            previousDescriptionRef.current = currentDesc; // Update after successful fetch
          })
          .catch((error) => {
            console.error('Failed to fetch suggestions:', error);
          });
      }
      // If description hasn't changed, do nothing (no request sent)
    }, interval);
    
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [nlDescription, interval, processing, setKeywordSuggestions, setExtractedItems]);
};
