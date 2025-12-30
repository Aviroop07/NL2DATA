import React from 'react';
import { Box } from '@mui/material';
import Layout from './components/layout/Layout';
import { useWebSocket } from './hooks/useWebSocket';
import { useAppStore } from './stores/useAppStore';

const App: React.FC = () => {
  const { jobId } = useAppStore();
  
  // Establish WebSocket connection when jobId is available
  useWebSocket(jobId);
  
  return (
    <Box sx={{ height: '100vh', width: '100vw', overflow: 'hidden' }}>
      <Layout />
    </Box>
  );
};

export default App;

