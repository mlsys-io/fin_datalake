import { create } from 'zustand'

export interface User {
    username: string
    email: string | null
    roles: string[]
    permissions: string[]
}

interface AuthState {
    user: User | null
    isAuthenticated: boolean
    isLoading: boolean
    setUser: (user: User | null) => void
    setLoading: (loading: boolean) => void
}

export const useAuthStore = create<AuthState>((set) => ({
    user: null,
    isAuthenticated: false,
    isLoading: true, // Start true while verifying session on load
    setUser: (user) => set({ user, isAuthenticated: !!user, isLoading: false }),
    setLoading: (loading) => set({ isLoading: loading }),
}))

// Helper to check if current user has a specific permission
export const hasPermission = (permission: string): boolean => {
    const user = useAuthStore.getState().user
    return user?.permissions.includes(permission) || false
}
