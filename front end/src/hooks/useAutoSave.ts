import { useEffect, useRef } from 'react';
import { AUTO_SAVE_INTERVAL } from '../utils/constants';

const STORAGE_KEY = 'nl2data_nl_description_draft';

export const useAutoSave = (value: string, interval: number = AUTO_SAVE_INTERVAL) => {
  const lastSavedRef = useRef<string>('');
  
  useEffect(() => {
    if (value === lastSavedRef.current) return;
    
    const timeoutId = setTimeout(() => {
      localStorage.setItem(STORAGE_KEY, value);
      lastSavedRef.current = value;
    }, interval);
    
    return () => clearTimeout(timeoutId);
  }, [value, interval]);
};

