import sys
import os
import codecs
import hashlib
import struct
import subprocess
import collections
import tensorflow as tf
from tensorflow.core.example import example_pb2


dm_single_close_quote = '\u2019' # unicode
dm_double_close_quote = '\u201d'
END_TOKENS = ['.', '!', '?', '...', "'", "`", '"', dm_single_close_quote, dm_double_close_quote, ")"] # acceptable ways to end a sentence

# We use these to separate the summary sentences in the .bin datafiles
SENTENCE_START = '<s>'
SENTENCE_END = '</s>'

all_test_story_fnames = 'story_fnames/test.txt'
all_eval_story_fnames = 'story_fnames/eval.txt'
all_train_story_fnames = 'story_fnames/train.txt'

twitter_tokenized_stories_dir = "./stories/"
finished_files_dir = "./finished_files/"
if not os.path.exists(finished_files_dir): os.mkdir(finished_files_dir)
chunks_dir = os.path.join(finished_files_dir, "chunked")

VOCAB_SIZE = 200000
CHUNK_SIZE = 1000 # num examples per chunk, for the chunked data


def chunk_file(set_name):
  in_file = 'finished_files/%s.bin' % set_name
  reader = codecs.open(in_file, "rb")
  chunk = 0
  finished = False
  while not finished:
    chunk_fname = os.path.join(chunks_dir, '%s_%03d.bin' % (set_name, chunk)) # new chunk
    with codecs.open(chunk_fname, 'wb') as writer:
      for _ in range(CHUNK_SIZE):
        len_bytes = reader.read(8)
        if not len_bytes:
          finished = True
          break
        str_len = struct.unpack('q', len_bytes)[0]
        example_str = struct.unpack('%ds' % str_len, reader.read(str_len))[0]
        writer.write(struct.pack('q', str_len))
        writer.write(struct.pack('%ds' % str_len, example_str))
      chunk += 1


def chunk_all():
  # Make a dir to hold the chunks
  if not os.path.isdir(chunks_dir):
    os.mkdir(chunks_dir)
  # Chunk the data
  for set_name in ['test']:#['train', 'val', 'test']:
    print("Splitting %s data into chunks..." % set_name)
    chunk_file(set_name)
  print("Saved chunked data in %s" % chunks_dir)



def read_text_file(text_file):
  lines = []
  with codecs.open(text_file, "r",encoding='utf-8') as f:
    for line in f:
      lines.append(line.strip())
  return lines


def fix_missing_period(line):
  """Adds a period to a line that is missing a period"""
  if "@highlight" in line: return line
  if line=="": return line
  if line[-1] in END_TOKENS: return line
  # print line[-1]
  return line + " ."


def get_art_abs(story_file):
  lines = read_text_file(story_file)

  # Lowercase everything
  lines = [line.lower() for line in lines]

  # Put periods on the ends of lines that are missing them (this is a problem in the dataset because many image captions don't end in periods; consequently they end up in the body of the article as run-on sentences)
  lines = [fix_missing_period(line) for line in lines]

  # Separate out article and abstract sentences
  article_lines = []
  highlights = []
  next_is_highlight = False
  for idx,line in enumerate(lines):
    if line == "":
      continue # empty line
    elif line.startswith("@highlight"):
      next_is_highlight = True
    elif next_is_highlight:
      highlights.append(line)
    else:
      article_lines.append(line)

  # Make article into a single string
  article = ' '.join(article_lines)

  # Make abstract into a signle string, putting <s> and </s> tags around the sentences
  abstract = ' '.join(["%s %s %s" % (SENTENCE_START, sent, SENTENCE_END) for sent in highlights])

  return article.encode('utf-8'), abstract.encode('utf-8')


def write_to_bin(story_fnames, out_file, makevocab=False):
  """Reads the tokenized .story files corresponding to the urls listed in the url_file and writes them to a out_file."""
  story_fnames = read_text_file(story_fnames)
  num_stories = len(story_fnames)

  if makevocab:
    vocab_counter = collections.Counter()

  with codecs.open(out_file, 'wb') as writer:
    for idx,s in enumerate(story_fnames):
      if idx % 1000 == 0:
        print("Writing story %i of %i; %.2f percent done" % (idx, num_stories, float(idx)*100.0/float(num_stories)))

      # Look in the tokenized story dirs to find the .story file corresponding to this url
      if os.path.isfile(os.path.join(twitter_tokenized_stories_dir, s)):
        story_file = os.path.join(twitter_tokenized_stories_dir, s)

      # Get the strings to write to .bin file
      article, abstract = get_art_abs(story_file)

      # Write to tf.Example
      tf_example = example_pb2.Example()
      tf_example.features.feature['article'].bytes_list.value.extend([article])
      tf_example.features.feature['abstract'].bytes_list.value.extend([abstract])
      tf_example_str = tf_example.SerializeToString()
      str_len = len(tf_example_str)
      writer.write(struct.pack('q', str_len))
      writer.write(struct.pack('%ds' % str_len, tf_example_str))

      # Write the vocab to file, if applicable
      if makevocab:
        art_tokens = article.split(' ')
        abs_tokens = abstract.split(' ')
        abs_tokens = [t for t in abs_tokens if t not in [SENTENCE_START, SENTENCE_END]] # remove these tags from vocab
        tokens = art_tokens + abs_tokens
        tokens = [t.strip() for t in tokens] # strip
        tokens = [t for t in tokens if t!=""] # remove empty
        vocab_counter.update(tokens)

  print("Finished writing file %s\n" % out_file)

  # write vocab to file
  if makevocab:
    print("Writing vocab file...")
    with codecs.open(os.path.join(finished_files_dir, "vocab"), 'w') as writer:
      for word, count in vocab_counter.most_common(VOCAB_SIZE):
        writer.write(word + ' ' + str(count) + '\n')
    print("Finished writing vocab file")


if __name__ == '__main__':
  write_to_bin(all_test_story_fnames, os.path.join(finished_files_dir, "test.bin"))
  write_to_bin(all_val_story_fnames, os.path.join(finished_files_dir, "val.bin"))
  write_to_bin(all_train_story_fnames, os.path.join(finished_files_dir, "train.bin"), makevocab=True)

  # Chunk the data. This splits each of train.bin, val.bin and test.bin into smaller chunks, each containing e.g. 1000 examples, and saves them in finished_files/chunks
  chunk_all()