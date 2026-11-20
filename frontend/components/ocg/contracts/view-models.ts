export type ChatImage = { id: string; name: string; media_type: string; url: string };

export type ChatImageUploadRequest = { project_id: string; name: string; data_url: string };

export type ChatModelSelection = { model: string; effort: string | null };
