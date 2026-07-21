import { runStorageDriverContract } from '../tests/storageDriverContract.ts'
import { createMemoryStorage } from './index.ts'

runStorageDriverContract('memory', () => createMemoryStorage())
