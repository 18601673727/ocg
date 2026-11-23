import { AttachmentTile, AttachmentTileRemove } from "../ui/attachment";
import type { ComposerDraft } from "./draft-state";

export default function AttachmentTiles(props: { draft: ComposerDraft }) {
  const {
    images,
    busy,
    uncertain,
    conflict,
    removeImage,
    attachments: { pendingImages, removePending },
  } = props.draft;
  return (
    <>
      {images().map((image) => (
        <AttachmentTile
          src={image.url}
          name={image.name}
          isComposer
          remove={
            <AttachmentTileRemove
              name={image.name}
              disabled={busy() || uncertain() || conflict()}
              onClick={() => removeImage(image.id)}
            />
          }
        />
      ))}
      {pendingImages().map((item) => (
        <AttachmentTile
          src={item.url}
          name={item.file.name}
          isComposer
          isUploading={!item.failed}
          isError={item.failed}
          errorMessage={item.failed ? "Upload failed" : undefined}
          remove={
            item.failed && (
              <AttachmentTileRemove name={item.file.name} onClick={() => removePending(item)} />
            )
          }
        />
      ))}
    </>
  );
}
