import React from 'react';
import { Box } from '@mui/material';
import Layout from './components/layout/Layout';

const App: React.FC = () => {
  // WebSocket removed - pipeline works via HTTP requests/responses only
  // WebSocket is only used for NL description suggestions (handled separately)
  
  return (
    <Box sx={{ height: '100vh', width: '100vw', overflow: 'hidden' }}>
      <Layout />
    </Box>
  );
};

export default App;

